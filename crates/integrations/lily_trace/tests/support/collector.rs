//! Own a real Collector container and retain its wire-format output as evidence.

use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};

// Collector Contrib 0.155.0, linux/amd64; no moving tag or implicit image pull.
pub const IMAGE: &str = "otel/opentelemetry-collector-contrib@sha256:4935caa35e9a4cb387e35732e8fb22b2b5759af8d12e7043357f03837f6e8df5";

pub struct Collector {
    name: String,
    created: bool,
    pub report: PathBuf,
}

impl Collector {
    pub fn new() -> Self {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::var_os("LILY_TEST_OTLP_REPORT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../target/otlp-qualification")
            });
        let report = root.join(&unique);
        std::fs::create_dir_all(report.join("capture")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // This directory is copied into the scratch image, where uid 10001
            // writes the files. No host directory is mounted into the container.
            std::fs::set_permissions(
                report.join("capture"),
                std::fs::Permissions::from_mode(0o777),
            )
            .unwrap();
        }
        std::fs::write(
            report.join("collector.yaml"),
            include_str!("collector.yaml"),
        )
        .unwrap();
        println!("OTLP qualification evidence: {}", report.display());
        Self {
            name: format!("lily-otlp-qualification-{unique}"),
            created: false,
            report,
        }
    }

    async fn docker(&self, arguments: &[&str]) -> Result<String, String> {
        let output = tokio::time::timeout(
            Duration::from_secs(20),
            Command::new("docker")
                .args(arguments)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| format!("docker {} exceeded 20 seconds", arguments[0]))?
        .map_err(|error| format!("docker {}: {error}", arguments[0]))?;
        if !output.status.success() {
            return Err(format!(
                "docker {} failed: {}{}",
                arguments[0],
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        String::from_utf8(output.stdout).map_err(|error| error.to_string())
    }

    pub async fn start(&mut self) -> Result<String, String> {
        let metadata = self.docker(&["image", "inspect", IMAGE, "--format",
            "{\"id\":{{json .Id}},\"digests\":{{json .RepoDigests}},\"architecture\":{{json .Architecture}},\"os\":{{json .Os}}}"]).await?;
        std::fs::write(self.report.join("image.json"), metadata).map_err(|e| e.to_string())?;
        self.docker(&[
            "create",
            "--pull=never",
            "--name",
            &self.name,
            "--label",
            "lily.qualification=otlp-methods",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--memory=256m",
            "--pids-limit=128",
            "--publish",
            "127.0.0.1::4317",
            "--publish",
            "127.0.0.1::13133",
            IMAGE,
            "--config=/lily-collector.yaml",
        ])
        .await?;
        self.created = true;
        self.docker(&[
            "cp",
            self.report.join("collector.yaml").to_str().unwrap(),
            &format!("{}:/lily-collector.yaml", self.name),
        ])
        .await?;
        self.docker(&[
            "cp",
            self.report.join("capture").to_str().unwrap(),
            &format!("{}:/capture", self.name),
        ])
        .await?;
        self.docker(&["start", &self.name]).await?;
        let health = self.port("13133/tcp").await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let ready = tokio::time::timeout(Duration::from_millis(500), async {
                let mut stream = tokio::net::TcpStream::connect(health).await?;
                stream
                    .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .await?;
                let mut response = [0; 128];
                let bytes = stream.read(&mut response).await?;
                Ok::<_, std::io::Error>(response[..bytes].starts_with(b"HTTP/1.1 200"))
            })
            .await;
            if matches!(ready, Ok(Ok(true))) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("Collector health check timed out".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let version = self
            .docker(&["exec", &self.name, "/otelcol-contrib", "--version"])
            .await?;
        std::fs::write(self.report.join("collector-version.txt"), version)
            .map_err(|e| e.to_string())?;
        Ok(format!("http://{}", self.port("4317/tcp").await?))
    }

    async fn port(&self, port: &str) -> Result<SocketAddr, String> {
        let binding = self.docker(&["port", &self.name, port]).await?;
        let address: SocketAddr = binding
            .trim()
            .parse()
            .map_err(|e| format!("invalid Docker binding: {e}"))?;
        if !address.ip().is_loopback() {
            return Err("Collector must be exposed only on loopback".into());
        }
        Ok(address)
    }

    pub async fn stop_and_capture(&self) -> Result<(), String> {
        // Graceful stop flushes Collector file exporters before we read them.
        self.docker(&["stop", "--time=10", &self.name]).await?;
        let state = self
            .docker(&["inspect", &self.name, "--format", "{{json .State}}"])
            .await?;
        std::fs::write(self.report.join("collector-state.json"), &state)
            .map_err(|e| e.to_string())?;
        let state: serde_json::Value = serde_json::from_str(&state).map_err(|e| e.to_string())?;
        if state["ExitCode"] != 0 || state["OOMKilled"] != false || state["Running"] != false {
            return Err(format!("Collector did not shut down cleanly: {state}"));
        }
        self.docker(&[
            "cp",
            &format!("{}:/capture/.", self.name),
            self.report.join("capture").to_str().unwrap(),
        ])
        .await?;
        Ok(())
    }

    pub async fn cleanup(&mut self) -> Result<(), String> {
        if self.created {
            // Preserve diagnostics on both success and failure. Removal is
            // scoped to this test's freshly created container, never a prune.
            let logs = Command::new("docker")
                .args(["logs", &self.name])
                .kill_on_drop(true)
                .output();
            if let Ok(Ok(output)) = tokio::time::timeout(Duration::from_secs(5), logs).await {
                let mut bytes = output.stdout;
                bytes.extend(output.stderr);
                let _ = std::fs::write(self.report.join("collector.log"), bytes);
            }
            self.docker(&["rm", "--force", &self.name]).await?;
            self.created = false;
        }
        Ok(())
    }
}
