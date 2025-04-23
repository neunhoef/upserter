# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `cargo build`
- Run: `cargo run`
- Release build: `cargo build --release`

## Lint/Test Commands
- Run all tests: `cargo test`
- Run single test: `cargo test test_name`
- Run tests in a module: `cargo test module_name`
- Lint: `cargo clippy`
- Format: `cargo fmt`

## Code Style Guidelines
- Follow Rust standard formatting with `cargo fmt`
- Use `cargo clippy` for code quality checks
- Use `snake_case` for variables and functions
- Use `CamelCase` for types and traits
- Prefer `Result<T, E>` for error handling with `?` operator
- Group imports by std, external crates, and internal modules
- Use explicit types for public API, inference for internal code
- Avoid unwrap() in production code; prefer proper error handling

## Async
- This program uses the tokio multithreaded runtime and is async throughout

## Usage of ArangoDB
- ArangoDB uses an HTTP/REST API
- Use the `reqwest` crate for sending requests to ArangoDB
