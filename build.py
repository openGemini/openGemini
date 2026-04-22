"""
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/v1.8.8/build.py

Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
"""
#!/usr/bin/env python3
import sys
import os
import platform
import subprocess
from datetime import datetime
import shutil
import re
import logging
import stat
import click
from typing import List, Dict, Optional, Tuple, Union

################
# Configuration Constants
################

PACKAGE_NAME = "openGemini"
GOBUILD_OUT = 'gobuild.txt'
PREREQS = ['git', 'go']
GO_VET_COMMAND = "go vet ./..."

TARGETS: Dict[str, str] = {
    'ts-sql': './app/ts-sql',
    'ts-meta': './app/ts-meta',
    'ts-store': './app/ts-store',
    'ts-server': './app/ts-server',
    'ts-monitor': './app/ts-monitor',
    'ts-data': './app/ts-data',
    'ts-recover': './app/ts-recover'
}

SUPPORTED_BUILDS: Dict[str, List[str]] = {
    'linux': ["amd64", "arm64"],
    'darwin': ["amd64", "arm64"],
    'windows': ["amd64", "arm64"],
}

################
# Logging Configuration
################

def init_logging(verbose: bool) -> None:
    """Initialize logging configuration"""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='[%(levelname)s] %(funcName)s: %(message)s'
    )
    logging.info(f"Writing error details to")

################
# Utility Functions
################

def run_command(
    command: str,
    allow_failure: bool = False,
    shell: bool = False
) -> Optional[str]:
    """Execute a shell command with error handling"""
    logging.debug(f"Executing command: {command}")
    try:
        if shell:
            if get_system_platform() != "windows":
                result = subprocess.check_output(
                    command,
                    stderr=subprocess.STDOUT,
                    shell=shell,
                    text=True
                )
            else:
                envs = os.environ.copy()
                cmd_parts = command.split()
                env_extra = {p.split("=")[0]: p.split("=")[1] for p in cmd_parts[:2] if "=" in p}
                envs.update(env_extra)
                command_new = ' '.join(cmd_parts[2:]) if len(cmd_parts) > 2 else ''

                result = subprocess.check_output(
                    command_new,
                    stderr=subprocess.STDOUT,
                    shell=shell,
                    env=envs,
                    text=True
                )
        else:
            result = subprocess.check_output(
                command.split(),
                stderr=subprocess.STDOUT,
                text=True
            )
        return result.strip()

    except subprocess.CalledProcessError as e:
        if allow_failure:
            logging.warning(f"Command failed: {command}, Error: {e.output}")
            return None
        else:
            logging.error(f"Command failed: {command}, Error: {e.output}")
            write_to_gobuild(e.output)
            sys.exit(1)
    except OSError as e:
        if allow_failure:
            logging.warning(f"Command environment error: {command}, Error: {e}")
            return None
        else:
            logging.error(f"Command environment error: {command}, Error: {e}")
            write_to_gobuild(str(e))
            sys.exit(1)

def write_to_gobuild(content: str) -> None:
    """Write build error information to log file"""
    logging.info(f"Writing error details to {GOBUILD_OUT}")
    try:
        with open(GOBUILD_OUT, 'w', encoding='utf-8') as f:
            f.write("error\n")
            f.write(content)
    except IOError as e:
        logging.error(f"Failed to write to {GOBUILD_OUT}: {e}")

def get_system_arch() -> str:
    """Get current system architecture in Go-compatible format"""
    arch = platform.uname()[4].lower()
    arch_map = {
        "x86_64": "amd64",
        "386": "i386",
        "aarch64": "arm64"
    }
    if arch in arch_map:
        return arch_map[arch]
    if 'arm' in arch:
        return "arm64"
    return arch

def get_system_platform() -> str:
    """Get current operating system in Go-compatible format"""
    if sys.platform.startswith("linux"):
        return "linux"
    elif sys.platform.startswith("win"):
        return "windows"
    return sys.platform

def local_changes_exist() -> bool:
    """Check for uncommitted local changes"""
    output = run_command("git diff-files --ignore-submodules --", allow_failure=True) or ""
    return len(output.strip()) > 0

def get_current_version() -> str:
    """Parse version information from git tag"""
    version_tag = run_command("git describe --always --tags --abbrev=0") or ""
    if isinstance(version_tag, str) and version_tag.startswith('v'):
        version_tag = version_tag[1:]
    return version_tag.replace("-", "~").replace("_", "~")

def get_current_commit(short: bool = False) -> str:
    """Get current git commit hash"""
    cmd = "git log --pretty=format:'%h' -n 1" if short else "git rev-parse HEAD"
    result = run_command(cmd)
    return result.strip('\'\n\r ') if result else ""

def get_current_branch() -> str:
    """Get current git branch name"""
    result = run_command("git rev-parse --abbrev-ref HEAD")
    return result.strip() if result else ""

def get_go_version() -> Optional[str]:
    """Get installed Go version"""
    out = run_command("go version")
    if not out:
        return None
    match = re.search(r'go version go(\S+)', out)
    return match.group(1).strip() if match else None

def check_dependency(binary: str) -> Optional[str]:
    """Check if a binary exists in system PATH"""
    def is_executable(path: str) -> bool:
        return os.path.isfile(path) and os.access(path, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        if get_system_platform() == "windows" and isinstance(path, str) and binary.upper() in path.upper():
            return path
        full_path = os.path.join(path, binary)
        if is_executable(full_path):
            return full_path
    return None

################
# Core Build Logic
################

def print_banner() -> None:
    """Print project banner"""
    banner = r"""
                                  ______                        _             _
                                 .' ___  |                      (_)           (_)
  .--.   _ .--.   .---.  _ .--. / .'   \_|  .---.  _ .--..--.   __   _ .--.   __
/ .'`\ \[ '/'`\ \/ /__\[ `.-. || |   ____ / /__\[ `.-. .-. | [  | [ `.-. | [  |
| \__. | | \__/ || \__., | | | |\ `.___]  || \__., | | | | | |  | |  | | | |  | |
 '.__.'  | ;.__/  '.__.'[___||__]`._____.'  '.__.'[___||__||__][___][___||__][___]
        [__|

  Build Script
"""
    logging.info(banner)

def check_environment(build_dir: Optional[str] = None) -> None:
    """Verify build environment is valid"""
    logging.info("Checking build environment...")

    # Check required dependencies
    for tool in PREREQS:
        if not check_dependency(tool):
            raise RuntimeError(f"Missing required dependency: {tool}. Please install it first.")

    # Verify Go installation
    go_version = get_go_version()
    if not go_version:
        raise RuntimeError("Could not determine Go version. Ensure Go is properly installed.")
    logging.info(f"Using Go version: {go_version}")

    # Log relevant environment variables
    for env_var in ["GOPATH", "GOBIN", "GOROOT"]:
        logging.debug(f"{env_var}: {os.environ.get(env_var, 'Not set')}")

def go_get_dependencies(branch: str, update: bool = False, no_uncommitted: bool = False) -> bool:
    """Retrieve or update Go dependencies"""
    if local_changes_exist() and no_uncommitted:
        logging.error("There are uncommitted changes. Commit or stash them first.")
        return False
    return True

def run_tests(
    race: bool = False,
    parallel: Optional[int] = None,
    timeout: Optional[str] = None,
    no_vet: bool = False,
    junit: bool = False
) -> bool:
    """Run test suite with optional race detection and reporting"""
    logging.info("Starting tests...")
    if race:
        logging.info("Race detection enabled")
    if parallel is not None:
        logging.info(f"Parallel test processes: {parallel}")
    if timeout is not None:
        logging.info(f"Test timeout: {timeout}")

    # Download module dependencies
    run_command("go mod download")

    # Check code formatting
    logging.info("Checking code formatting with go fmt...")
    fmt_output = run_command("go fmt ./...")
    if fmt_output:
        logging.error("Code formatting issues found. Run 'go fmt ./...' to fix.")
        logging.error(fmt_output)
        return False

    # Run go vet if enabled
    if not no_vet:
        logging.info("Running code analysis with go vet...")
        vet_output = run_command(GO_VET_COMMAND)
        if vet_output:
            logging.error("go vet found issues. Fix them before proceeding.")
            logging.error(vet_output)
            write_to_gobuild(vet_output)
            return False
    else:
        logging.info("Skipping go vet as requested")

    # Build test command
    test_cmd_parts = ["go test -v"]
    if race:
        test_cmd_parts.append("-race")
    if parallel is not None:
        test_cmd_parts.append(f"-parallel {parallel}")
    if timeout is not None:
        test_cmd_parts.append(f"-timeout {timeout}")
    test_cmd_parts.append("./...")
    full_test_cmd = " ".join(test_cmd_parts)

    # Execute tests
    if junit:
        return _run_tests_with_junit(full_test_cmd)
    else:
        return _run_tests_normal(full_test_cmd)

def _run_tests_normal(test_cmd: str) -> bool:
    """Run tests in normal mode"""
    try:
        output = run_command(test_cmd)
        if output and "FAIL" in output:
            write_to_gobuild(output)
            return False
        logging.debug(f"Test output:\n{output}")
        return True
    except Exception as e:
        logging.error(f"Test execution failed: {e}")
        return False

def _run_tests_with_junit(test_cmd: str) -> bool:
    """Run tests and generate JUnit-style report"""
    logging.info("Installing go-junit-report...")
    run_command("go get github.com/jstemmer/go-junit-report")

    try:
        # Execute tests and capture output as string
        proc = subprocess.Popen(
            test_cmd.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        output, _ = proc.communicate(timeout=30)

        # Generate JUnit report
        with open('test-results.xml', 'w', encoding='utf-8') as f:
            junit_proc = subprocess.Popen(
                ["go-junit-report"],
                stdin=subprocess.PIPE,
                stdout=f,
                stderr=subprocess.PIPE,
                text=True
            )
            _, err = junit_proc.communicate(output)
            if junit_proc.returncode != 0:
                logging.error(f"Failed to generate JUnit report: {err}")
                return False

        if proc.returncode != 0:
            logging.error(f"Tests failed: {output}")
            return False
        return True
    except Exception as e:
        logging.error(f"Test execution failed: {e}")
        return False

def build_targets(
    platform: str,
    arch: str,
    version: str,
    clean: bool = False,
    outdir: str = ".",
    tags: List[str] = None,
    static: bool = False,
    race: bool = False
) -> None:
    """Build targets for specified platform and architecture"""
    tags = tags or []
    if platform not in SUPPORTED_BUILDS:
        raise ValueError(f"Unsupported platform: {platform}")
    if arch not in SUPPORTED_BUILDS[platform]:
        raise ValueError(f"Unsupported architecture {arch} for platform {platform}")

    # Prepare output directory
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif clean and outdir not in ('/', '.'):
        logging.info(f"Cleaning build directory: {outdir}")
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    logging.info(f"Building for {platform}/{arch}...")
    logging.info(f"Output directory: {outdir}")

    # Build each target
    for target_name, target_path in TARGETS.items():
        # Add .exe extension for Windows
        output_filename = f"{target_name}.exe" if platform == "windows" else target_name
        output_path = os.path.join(outdir, output_filename)

        # Construct build command
        build_cmd_parts = []

        # Handle static build
        if static or "static_" in arch:
            if "static_" in arch:
                static = True
                arch = arch.replace("static_", "")
            build_cmd_parts.append("CGO_ENABLED=0")

        # Set GOOS and GOARCH
        build_cmd_parts.append(f"GOOS={platform} GOARCH={arch}")

        # Handle ARM specific flags
        if "arm" in arch and platform != "windows":
            if arch == "armel":
                build_cmd_parts.append("GOARM=5")
            elif arch in ("armhf", "arm"):
                build_cmd_parts.append("GOARM=6")
            elif arch == "arm64":
                build_cmd_parts.append("GOARM=7")

        # Base build command
        if "arm" in arch:
            build_cmd_parts.append(f"go build -o {output_path}")
        else:
            build_cmd_parts.append(f"go build -mod=mod -o {output_path}")

        # Add build tags and race detection
        if race:
            build_cmd_parts.append("-race")
        if tags:
            build_cmd_parts.append(f"-tags {','.join(tags)}")

        # Add linker flags with version info
        ldflags = (
            f"-ldflags=\"-X github.com/openGemini/openGemini/app.Version={version} "
            f"-X github.com/openGemini/openGemini/app.GitBranch={get_current_branch()} "
            f"-X github.com/openGemini/openGemini/app.GitCommit={get_current_commit()}\""
        )
        build_cmd_parts.append(ldflags)

        # Add source path
        build_cmd_parts.append(target_path)

        # Execute build
        build_cmd = " ".join(build_cmd_parts)
        start_time = datetime.utcnow()
        logging.info(f"Building {target_name}: {build_cmd}")
        run_command(build_cmd, shell=True)
        end_time = datetime.utcnow()
        logging.info(f"Build completed in {(end_time - start_time).total_seconds():.2f}s")


# Initialize logging
init_logging(True)
################
# Click Command Configuration
################

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--outdir', '-o',
              default='./build/',
              type=click.Path(),
              help='Output directory for built binaries')
@click.option('--name', '-n',
              default=PACKAGE_NAME,
              help='Name used for packaging')
@click.option('--arch',
              default=get_system_arch(),
              help=f'Target architecture (supported: {", ".join(sum(SUPPORTED_BUILDS.values(), []))})')
@click.option('--platform',
              default=get_system_platform(),
              help=f'Target platform (supported: {", ".join(SUPPORTED_BUILDS.keys())})')
@click.option('--branch',
              default=get_current_branch(),
              help='Git branch to build from')
@click.option('--commit',
              default=get_current_commit(short=True),
              help='Git commit to build from')
@click.option('--version',
              default=get_current_version(),
              help='Version string for the build (e.g., 0.1.0)')
@click.option('--iteration',
              default="1",
              help='Package iteration number')
@click.option('--nightly',
              is_flag=True,
              help='Mark build as nightly (increments minor version)')
@click.option('--update',
              is_flag=True,
              help='Update dependencies before building')
@click.option('--release',
              is_flag=True,
              help='Mark build as official release')
@click.option('--clean',
              is_flag=True,
              help='Clean output directory before building')
@click.option('--no-get',
              is_flag=True,
              help='Skip retrieving dependencies')
@click.option('--no-uncommitted',
              is_flag=True,
              help='Fail if uncommitted changes exist')
@click.option('--build-tags',
              help='Comma-separated list of build tags')
@click.option('--static',
              is_flag=True,
              help='Build statically linked binaries')
@click.option('--test',
              is_flag=True,
              help='Run tests instead of building')
@click.option('--junit-report',
              is_flag=True,
              help='Generate JUnit-style test report')
@click.option('--no-vet',
              is_flag=True,
              help='Skip go vet during tests')
@click.option('--race',
              is_flag=True,
              help='Enable race detection in tests/builds')
@click.option('--parallel',
              type=int,
              help='Number of parallel test processes')
@click.option('--timeout',
              help='Test timeout (e.g., 30s)')

def main(
    outdir: str,
    name: str,
    arch: str,
    platform: str,
    branch: str,
    commit: str,
    version: str,
    iteration: str,
    nightly: bool,
    update: bool,
    release: bool,
    clean: bool,
    no_get: bool,
    no_uncommitted: bool,
    build_tags: str,
    static: bool,
    test: bool,
    junit_report: bool,
    no_vet: bool,
    race: bool,
    parallel: int,
    timeout: str
) -> None:
    """openGemini build and packaging script"""

    try:
        # Validate mutually exclusive options
        if release and nightly:
            raise click.BadParameter("Cannot specify both --release and --nightly")

        # Handle nightly versioning
        if nightly:
            ver_parts = version.split('.')
            if len(ver_parts) == 3:
                try:
                    ver_parts[1] = str(int(ver_parts[1]) + 1)
                    ver_parts[2] = "0"
                    version = ".".join(ver_parts)
                except (ValueError, IndexError):
                    logging.warning("Could not increment version, using original")
            version = f"{version}~n{datetime.utcnow().strftime('%Y%m%d%H%M')}"

        # Process build tags
        build_tags_list = build_tags.split(',') if build_tags else []

        # Store original git state
        orig_commit = get_current_commit(short=True)
        orig_branch = get_current_branch()

        # Validate platform
        if platform not in SUPPORTED_BUILDS and platform != 'all':
            raise click.BadParameter(f"Unsupported platform: {platform}. Supported: {', '.join(SUPPORTED_BUILDS.keys())}")

        # Checkout specified branch/commit if needed
        if branch != orig_branch and commit != orig_commit:
            raise click.BadParameter("Specify either --branch or --commit, not both")
        elif branch != orig_branch:
            logging.info(f"Checking out branch: {branch}")
            run_command(f"git checkout {branch}")
        elif commit != orig_commit:
            logging.info(f"Checking out commit: {commit}")
            run_command(f"git checkout {commit}")

        # Check environment and dependencies
        check_environment()
        if not no_get:
            if not go_get_dependencies(branch, update, no_uncommitted):
                sys.exit(1)

        # Run tests if requested
        if test:
            if not run_tests(race, parallel, timeout, no_vet, junit_report):
                sys.exit(1)
            sys.exit(0)

        # Determine build platforms and architectures
        platforms = SUPPORTED_BUILDS.keys() if platform == 'all' else [platform]
        single_build = platform != 'all' and arch != 'all'

        for plf in platforms:
            archs = SUPPORTED_BUILDS[plf] if arch == 'all' else [arch]

            for arch_target in archs:
                # Determine output directory
                output_dir = outdir
                if not single_build:
                    output_dir = os.path.join(outdir, plf, arch_target)

                # Build targets
                build_targets(
                    platform=plf,
                    arch=arch_target,
                    version=version,
                    clean=clean,
                    outdir=output_dir,
                    tags=build_tags_list,
                    static=static,
                    race=race
                )

        # Restore original git branch
        if orig_branch != get_current_branch():
            logging.info(f"Restoring original branch: {orig_branch}")
            run_command(f"git checkout {orig_branch}")

        logging.info("Build completed successfully!")

    except Exception as e:
        logging.error(f"Build failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    print_banner()
    main()
