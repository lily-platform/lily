use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::visit_mut::VisitMut;
use syn::{Block, Expr, FnArg, ItemFn, Meta, Pat, ReturnType, Stmt, Type};

use crate::utils::{parse_trace_arguments, TraceArguments};

pub(crate) fn lily_trace_impl(args: Vec<Meta>, mut input: ItemFn) -> TokenStream {
    let arguments = match parse_trace_arguments(&args, &input) {
        Ok(arguments) => arguments,
        Err(error) => return error.to_compile_error().into(),
    };
    let span_name = arguments
        .span_name
        .clone()
        .unwrap_or_else(|| input.sig.ident.to_string());
    let fields = build_field_records(
        &input.sig.inputs,
        &arguments.fields,
        &arguments.skip_fields,
        &arguments.crate_path,
    );
    let output = inferred_output(&input.sig.output);
    let generated = if input.sig.asyncness.is_some() {
        instrument_body(
            &input.block,
            true,
            Some(&output),
            &arguments,
            &span_name,
            &fields,
        )
    } else if let Some(body) = returned_async_body(&mut input.block) {
        // async-trait has already erased `async` from the signature. Patch the
        // actual future body and preserve its original box, bounds and lifetimes.
        // async-trait already supplies the output-type inference edge inside
        // this body. Its outer signature describes the boxed future instead.
        let generated = instrument_body(body, true, None, &arguments, &span_name, &fields);
        *body = syn::parse_quote!({ #generated });
        return quote!(#input).into();
    } else {
        instrument_body(
            &input.block,
            false,
            Some(&output),
            &arguments,
            &span_name,
            &fields,
        )
    };
    input.block = syn::parse_quote!({ #generated });
    quote!(#input).into()
}

fn inferred_output(output: &ReturnType) -> Type {
    struct InferOpaque;
    impl VisitMut for InferOpaque {
        fn visit_type_mut(&mut self, value: &mut Type) {
            if matches!(value, Type::ImplTrait(_)) {
                *value = syn::parse_quote!(_);
            } else {
                syn::visit_mut::visit_type_mut(self, value);
            }
        }
    }
    let mut output = match output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, value) => *value.clone(),
    };
    InferOpaque.visit_type_mut(&mut output);
    output
}

/// Recognize the returned future, never unrelated async blocks in the body.
fn returned_async_body(block: &mut Block) -> Option<&mut Block> {
    let Stmt::Expr(expression, _) = block.stmts.last_mut()? else {
        return None;
    };
    async_expression_body(expression)
}

fn async_expression_body(expression: &mut Expr) -> Option<&mut Block> {
    match expression {
        Expr::Async(expression) => Some(&mut expression.block),
        Expr::Paren(expression) => async_expression_body(&mut expression.expr),
        Expr::Group(expression) => async_expression_body(&mut expression.expr),
        Expr::Return(expression) => async_expression_body(expression.expr.as_mut()?),
        Expr::Call(expression) if expression.args.len() == 1 => {
            let Expr::Path(function) = expression.func.as_ref() else {
                return None;
            };
            let mut path = function.path.segments.iter().rev();
            if path.next()?.ident != "pin" || path.next()?.ident != "Box" {
                return None;
            }
            async_expression_body(expression.args.first_mut()?)
        }
        _ => None,
    }
}

fn instrument_body(
    body: &Block,
    is_async: bool,
    output: Option<&Type>,
    arguments: &TraceArguments,
    span_name: &str,
    fields: &[proc_macro2::TokenStream],
) -> proc_macro2::TokenStream {
    let trace_crate = &arguments.crate_path;
    let statements = &body.stmts;
    let level = Ident::new(&arguments.level, Span::call_site());
    // Mixed-site bindings do not shadow same-named application arguments.
    let span = Ident::new("__lily_span", Span::mixed_site());
    let guard = Ident::new("__lily_operation", Span::mixed_site());
    let value = Ident::new("__lily_result", Span::mixed_site());
    let entered = Ident::new("__lily_entered", Span::mixed_site());
    let instrumented_span = Ident::new("__lily_instrumented_span", Span::mixed_site());
    let event_span = Ident::new("__lily_event_span", Span::mixed_site());
    let event = Ident::new("__lily_event", Span::mixed_site());
    let type_hint = Ident::new("__lily_output_hint", Span::mixed_site());
    let environment = if arguments.environments.is_empty() {
        quote!(true)
    } else {
        let environments = &arguments.environments;
        quote!(#trace_crate::environment_matches(&[#(#environments),*]))
    };
    let execute = if is_async {
        // Like async-trait's generated body, this unreachable return preserves
        // contextual coercions (trait objects, slices, and `?` conversions).
        // It establishes the async block's output before any user return.
        // `!` is not a stable generic argument on our minimum Rust version.
        let hint = output
            .filter(|ty| !matches!(ty, Type::Never(_)))
            .map(|output| {
                quote! {
                    if let ::core::option::Option::Some(#type_hint) =
                        ::core::option::Option::None::<#output>
                    {
                        return #type_hint;
                    }
                }
            });
        quote!(async move { #hint #(#statements)* }.await)
    } else {
        let output = output.map(|output| quote!(-> #output));
        quote!((move || #output #body)())
    };
    let finish = if arguments.capture_result {
        quote!(#guard.finish_result(&#value);)
    } else {
        quote!(#guard.finish();)
    };
    let owned_span = if is_async {
        quote!(#span)
    } else {
        quote!(#span.clone())
    };
    let observed = quote! {
        let #guard = #trace_crate::__private::OperationGuard::start(
            #owned_span,
            |#event_span: &#trace_crate::tracing::Span,
             #event: #trace_crate::__private::LifecycleEvent| {
                if #event.phase == "started" {
                    #trace_crate::tracing::event!(
                        name: "lily.method.started", parent: #event_span,
                        #trace_crate::tracing::Level::#level,
                        lily.operation = #span_name,
                        lily.lifecycle = "started",
                        "method started"
                    );
                } else {
                    #trace_crate::tracing::event!(
                        name: "lily.method.finished", parent: #event_span,
                        #trace_crate::tracing::Level::#level,
                        lily.operation = #span_name,
                        lily.lifecycle = #event.phase,
                        lily.duration_ms = #event.duration_ms,
                        lily.outcome = #event.outcome,
                        lily.error_code = #event.error_code,
                        "method finished"
                    );
                }
            },
        );
        #[allow(clippy::redundant_closure_call)]
        let #value = #execute;
        #finish
        #value
    };
    let observed = if is_async {
        quote! {
            let #instrumented_span = #span.clone();
            #trace_crate::tracing::Instrument::instrument(
                async move { #observed }, #instrumented_span
            ).await
        }
    } else {
        quote! {
            let #entered = #span.enter();
            #observed
        }
    };
    quote! {
        if #environment {
            let #span = #trace_crate::tracing::span!(
                #trace_crate::tracing::Level::#level, #span_name,
                lily.instrumentation = "method",
                lily.lifecycle = #trace_crate::tracing::field::Empty,
                lily.duration_ms = #trace_crate::tracing::field::Empty,
                lily.outcome = #trace_crate::tracing::field::Empty,
                lily.error_code = #trace_crate::tracing::field::Empty,
                otel.status_code = #trace_crate::tracing::field::Empty
                #(, #fields)*
            );
            if #span.is_disabled() {
                #(#statements)*
            } else {
                #observed
            }
        } else {
            #(#statements)*
        }
    }
}

fn build_field_records(
    inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
    fields: &[String],
    skip: &[String],
    trace_crate: &syn::Path,
) -> Vec<proc_macro2::TokenStream> {
    inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    let ident = &pat_ident.ident;
                    let ident_str = ident.to_string();

                    // Skip self and skipped fields
                    if ident_str == "self"
                        || !fields.contains(&ident_str)
                        || skip.contains(&ident_str)
                    {
                        return None;
                    }

                    return Some(quote! {
                        #ident_str = #trace_crate::tracing::field::debug(&#ident)
                    });
                }
            }
            None
        })
        .collect()
}
