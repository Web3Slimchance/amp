---
name: "app-ampup"
description: "ampup version manager and installer for ampd and ampctl binaries. Load when asking about installing Amp, managing versions, building from source, or the ampup CLI"
type: feature
status: "stable"
components: "app:ampup"
---

# ampup - Version Manager & Installer

## Summary

ampup is the official version manager and installer for the Amp toolchain. It downloads, installs, and manages multiple versions of `ampd` and `ampctl`, allowing developers to switch between versions seamlessly. ampup supports installing from prebuilt binaries, building from source (including specific branches, PRs, and commits), and self-updating.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Configuration](#configuration)
4. [Limitations](#limitations)
5. [References](#references)

## Key Concepts

- **Version Manager**: ampup manages multiple installed versions of `ampd` and `ampctl` side-by-side, using symlinks to activate the selected version
- **Binary Distribution**: Prebuilt binaries are downloaded from GitHub releases for supported platforms (Linux x86_64/aarch64, macOS aarch64)
- **Amp Directory**: The `~/.amp/` directory stores all ampup-managed binaries, version metadata, and symlinks

## Usage

### Installing ampup

Install ampup and the latest Amp release with a single command:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
```

This will:
1. Download and install the `ampup` binary to `~/.amp/bin/`
2. Configure your shell PATH automatically
3. Install the latest versions of `ampd` and `ampctl`

You may need to restart your terminal or run `source ~/.zshenv` (or your shell's equivalent) to update your PATH.

#### Install Options

```bash
# Custom installation directory
curl ... | sh -s -- --install-dir /custom/path

# Skip PATH modification
curl ... | sh -s -- --no-modify-path

# Skip installing the latest version
curl ... | sh -s -- --no-install-latest
```

### Managing Versions

```bash
# Install or update to the latest version
ampup install

# Install a specific version
ampup install v0.1.0

# List all installed versions
ampup list

# Switch to a specific installed version
ampup use v0.1.0

# Uninstall a specific version
ampup uninstall v0.1.0
```

### Building from Source

ampup can clone the repository and compile binaries from source:

```bash
# Build from the main branch
ampup build

# Build from a specific branch
ampup build --branch develop

# Build from a specific pull request
ampup build --pr 123

# Build from a specific commit
ampup build --commit abc1234

# Build from a local repository
ampup build --path /path/to/amp

# Build from a custom repository
ampup build --repo myorg/amp-fork

# Custom version label for the build
ampup build --name my-custom-build

# Control build parallelism
ampup build --jobs 4
```

### Updating

```bash
# Update ampd and ampctl to the latest version
ampup update

# Update ampup itself
ampup self update

# Print ampup version
ampup self version
```

### Alternative: Installation via Nix

For Nix users, `ampd` is available as a flake without needing ampup:

```bash
# Run directly without installing
nix run github:edgeandnode/amp

# Install to your profile
nix profile install github:edgeandnode/amp

# Try it out temporarily
nix shell github:edgeandnode/amp -c ampd --version
```

Add to a NixOS or home-manager configuration:

```nix
{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    amp = {
      url = "github:edgeandnode/amp";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, amp, ... }: {
    # NixOS
    environment.systemPackages = [
      amp.packages.${system}.ampd
    ];

    # Or home-manager
    home.packages = [
      amp.packages.${system}.ampd
    ];
  };
}
```

### Building Manually (Without ampup)

For those who prefer manual compilation:

```bash
cargo build --release -p ampd
# Binary available at target/release/ampd
```

## Configuration

### Environment Variables

| Variable       | Description                                              |
|----------------|----------------------------------------------------------|
| `GITHUB_TOKEN` | GitHub personal access token for private repository access |
| `AMP_DIR`      | Override the default installation directory (`~/.amp/`)   |
| `AMP_REPO`     | Override the default repository (`edgeandnode/amp`)       |

### Directory Structure

ampup organizes its files under `~/.amp/`:

```
~/.amp/
├── bin/
│   ├── ampup           # Version manager binary
│   ├── ampd            # Symlink to active ampd version
│   └── ampctl          # Symlink to active ampctl version
├── versions/
│   ├── v0.1.0/
│   │   ├── ampd        # ampd binary for v0.1.0
│   │   └── ampctl      # ampctl binary for v0.1.0
│   └── v0.2.0/
│       ├── ampd
│       └── ampctl
└── .version            # Tracks the currently active version
```

### GitHub Token Resolution

ampup resolves GitHub authentication tokens in this order:

1. Explicit `--github-token` flag or `GITHUB_TOKEN` environment variable
2. Automatic fallback to `gh auth token` (GitHub CLI)
3. Unauthenticated mode (lower API rate limits)

## Limitations

- macOS support is limited to Apple Silicon (aarch64) for prebuilt binaries
- Building from source requires a working Rust toolchain
- GitHub API rate limits apply when downloading releases without a token

## References

- [app-ampd](app-ampd.md) - Related: The daemon binary that ampup installs and manages
- [app-ampctl](app-ampctl.md) - Related: The CLI tool that ampup installs and manages
