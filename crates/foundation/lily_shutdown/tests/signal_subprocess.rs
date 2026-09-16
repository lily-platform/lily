#[cfg(unix)]
#[test]
#[ignore = "release qualification: requires host OS signal delivery"]
fn unix_first_and_second_signal_semantics_are_observable_in_a_subprocess() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let mut child = Command::new(env!("CARGO_BIN_EXE_lily_shutdown_signal_probe"))
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn signal probe");
    let stdout = child.stdout.take().expect("probe stdout");
    let (line_tx, line_rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line_tx.send(line).is_err() {
                break;
            }
        }
    });
    let next_line = || {
        line_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("signal probe output timeout")
            .expect("read signal probe output")
    };
    assert_eq!(next_line(), "READY");

    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(next_line(), "FIRST:Terminate");
    assert_eq!(next_line(), "ADMISSION:false");

    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(next_line(), "FORCED");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            panic!("signal probe did not stop after force escalation");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
