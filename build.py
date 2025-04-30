#!/usr/bin/python2.7 -u

import sys
import os
import platform
import subprocess
from datetime import datetime
import shutil
import re
import logging
import argparse
import stat

################
#### OpenGemini Variables
################

# Packaging variables
PACKAGE_NAME = "OpenGemini"
gobuild_out = 'gobuild.txt'
prereqs = [ 'git', 'go' ]
go_vet_command = "go vet ./..."

targets = {
    'ts-sql' : './app/ts-sql',
    'ts-meta' : './app/ts-meta',
    'ts-store' : './app/ts-store',
    'ts-server' : './app/ts-server',
    'ts-monitor' : './app/ts-monitor',
    'ts-data' : './app/ts-data',
    'ts-recover': './app/ts-recover'
}

supported_builds = {
    'linux': [ "amd64","arm64"],
    'darwin': [ "amd64","arm64"],
    'windows': [ "amd64","arm64"],
}

################
#### OpenGemini Functions
################

def print_banner():
    logging.info("""
                                  ______                        _             _
                                 .' ___  |                      (_)           (_)
  .--.   _ .--.   .---.  _ .--. / .'   \_|  .---.  _ .--..--.   __   _ .--.   __
/ .'`\ \[ '/'`\ \/ /__\[ `.-. || |   ____ / /__\[ `.-. .-. | [  | [ `.-. | [  |
| \__. | | \__/ || \__., | | | |\ `.___]  || \__., | | | | | |  | |  | | | |  | |
 '.__.'  | ;.__/  '.__.'[___||__]`._____.'  '.__.'[___||__||__][___][___||__][___]
        [__|

  Build Script
""")

def go_get(branch, update=False, no_uncommitted=False):
    """Retrieve build dependencies or restore pinned dependencies.
    """
    if local_changes() and no_uncommitted:
        logging.error("There are uncommitted changes in the current directory.")
        return False
    return True

def run_tests(race, parallel, timeout, no_vet, junit=False):
    """Run the Go test suite on binary output.
    """
    logging.info("Starting tests...")
    if race:
        logging.info("Race is enabled.")
    if parallel is not None:
        logging.info("Using parallel: %s", parallel)
    if timeout is not None:
        logging.info("Using timeout: %s", parallel)

    logging.info("Fetching module dependencies...")
    run("go mod download")

    logging.info("Ensuring code is properly formatted with go fmt...")
    out = run("go fmt ./...")
    if len(out) > 0:
        logging.error("Code not formatted. Please use 'go fmt ./...' to fix formatting errors.")
        logging.error(out)
        return False
    if not no_vet:
        logging.info("Running 'go vet'...")
        out = run(go_vet_command)
        if len(out) > 0:
            logging.error("Go vet failed. Please run 'go vet ./...' and fix any errors.")
            logging.error(out)
            write_to_gobuild(out)
            return False
    else:
        logging.info("Skipping 'go vet' call...")
    test_command = "go test -v"
    if race:
        test_command += " -race"
    if parallel is not None:
        test_command += " -parallel {}".format(parallel)
    if timeout is not None:
        test_command += " -timeout {}".format(timeout)
    test_command += " ./..."
    if junit:
        logging.info("Retrieving go-junit-report...")
        run("go get github.com/jstemmer/go-junit-report")

        # Retrieve the output from this command.
        logging.info("Running tests...")
        logging.debug(test_command)
        proc = subprocess.Popen(test_command.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output, unused_err = proc.communicate(timeout=30)
        output = output.decode('utf-8').strip()

        file = 'test-results.xml'
        flags = os.O_WRONLY | os.O_CREAT
        modes = stat.S_IWUSR | stat.S_IRUSR
        # Process the output through go-junit-report.
        with os.fdopen(os.open(file, flags, modes), 'w', encoding="utf-8") as f:
            logging.debug("go-junit-report")
            junit_proc = subprocess.Popen(["go-junit-report"], stdin=subprocess.PIPE, stdout=f, stderr=subprocess.PIPE)
            unused_output, err = junit_proc.communicate(output.encode('ascii', 'ignore'), timeout=30)
            if junit_proc.returncode != 0:
                logging.error("Command '%s' failed with error: %s", "go-junit-report", err)
                sys.exit(1)

        if proc.returncode != 0:
            logging.error("Command '%s' failed with error: %s", test_command, output.encode('ascii', 'ignore'))
            sys.exit(1)
    else:
        logging.info("Running tests...")
        output = run(test_command)
        if "FAIL" in output:
            write_to_gobuild(output)
        logging.debug("Test output:\n%s", out.encode('ascii', 'ignore'))
    return True

################
#### All OpenGemini-specific content above this line
################

def run(command, allow_failure=False, shell=False):
    """Run shell command (convenience wrapper around subprocess).
    """
    out = None

    logging.debug(command)
    try:
        if shell:
            if get_system_platform() != "windows":
                out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=shell)
            else:
                envs = os.environ.copy()
                env_extra = {env.split("=")[0]:env.split("=")[1] for env in command.split()[:2]}
                envs.update(env_extra)
                command_new = ' '.join(command.split()[2:]) # remove GOOS=windows GOARCH=arm64
                out = subprocess.check_output(command_new, stderr=subprocess.STDOUT, shell=shell, env=envs)
        else:
            out = subprocess.check_output(command.split(), stderr=subprocess.STDOUT)
        out = out.decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        if allow_failure:
            logging.warning("Command '%s' failed with error: %s", command, e.output)
            return None
        else:
            logging.error("Command '%s' failed with error: %s", command, e.output)
            write_to_gobuild(e.output)
            sys.exit(1)
    except OSError as e:
        if allow_failure:
            logging.warning("Command '%s' failed with error: %s", command, e)
            return out
        else:
            logging.error("Command '%s' failed with error: %s", command, e)
            write_to_gobuild(e.output)
            sys.exit(1)
    else:
        return out

def increment_minor_version(version):
    """Return the version with the minor version incremented and patch
    version set to zero.
    """
    ver_list = version.split('.')
    if len(ver_list) != 3:
        logging.warning("Could not determine how to increment version '%s', will just use provided version.", version)
        return version
    ver_list[1] = str(int(ver_list[1]) + 1)
    ver_list[2] = str(0)
    inc_version = '.'.join(ver_list)
    logging.debug("Incremented version from '%s' to '%s'.", version, inc_version)
    return inc_version

def get_current_version_tag():
    """Retrieve the raw git version tag.
    """
    version = run("git describe --always --tags --abbrev=0")
    return version

def get_current_version():
    """Parse version information from git tag output.
    """
    version_tag = get_current_version_tag()
    # Remove leading 'v'
    if version_tag[0] == 'v':
        version_tag = version_tag[1:]
    # Replace any '-'/'_' with '~'
    if '-' in version_tag:
        version_tag = version_tag.replace("-","~")
    if '_' in version_tag:
        version_tag = version_tag.replace("_","~")
    return version_tag

def get_current_commit(short=False):
    """Retrieve the current git commit.
    """
    command = None
    if short:
        command = "git log --pretty=format:'%h' -n 1"
    else:
        command = "git rev-parse HEAD"
    out = run(command)
    return out.strip('\'\n\r ')

def get_current_branch():
    """Retrieve the current git branch.
    """
    command = "git rev-parse --abbrev-ref HEAD"
    out = run(command)
    return out.strip()

def local_changes():
    """Return True if there are local un-committed changes.
    """
    output = run("git diff-files --ignore-submodules --").strip()
    if len(output) > 0:
        return True
    return False

def get_system_arch():
    """Retrieve current system architecture.
    """
    arch = platform.uname()[4].lower()
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "386":
        arch = "i386"
    elif arch == "aarch64":
        arch = "arm64"
    elif 'arm' in arch:
        # Prevent uname from reporting full ARM arch (eg 'armv7l')
        arch = "arm64"
    return arch

def get_system_platform():
    """Retrieve current system platform.
    """
    if sys.platform.startswith("linux"):
        return "linux"
    elif sys.platform.startswith("win"):
        return "windows"
    else:
        return sys.platform

def get_go_version():
    """Retrieve version information for Go.
    """
    out = run("go version")
    matches = re.search('go version go(\S+)', out)
    if matches is not None:
        return matches.groups()[0].strip()
    return None

def check_path_for(b):
    """Check the the user's path for the provided binary.
    """
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        if b.upper() in path.upper():
            return path#windows
        full_path = os.path.join(path, b)
        if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
            return full_path

def check_environ(build_dir = None):
    """Check environment for common Go variables.
    """
    logging.info("Checking environment...")
    for v in [ "GOPATH", "GOBIN", "GOROOT" ]:
        logging.debug("Using '%s' for %s", os.environ.get(v), v)

    cwd = os.getcwd()
    if build_dir is None and os.environ.get("GOPATH") and os.environ.get("GOPATH") in cwd:
        logging.warning("Your current directory is under your GOPATH. This may lead to build failures when using modules.")
    return True

def check_prereqs():
    """Check user path for required dependencies.
    """
    logging.info("Checking for dependencies...")
    for req in prereqs:
        if not check_path_for(req):
            logging.error("Could not find dependency: %s", req)
            return False
    return True

def build(version=None,
          sys_platform=None,
          arch=None,
          nightly=False,
          race=False,
          clean=False,
          outdir=".",
          tags=None,
          static=False):
    """Build each target for the specified architecture and platform.
    """
    if tags is None:
        tags = []
    logging.info("Starting build for %s/%s...", sys_platform, arch)
    logging.info("Using Go version: %s", get_go_version())
    logging.info("Using git branch: %s", get_current_branch())
    logging.info("Using git commit: %s", get_current_commit())
    if static:
        logging.info("Using statically-compiled output.")
    if race:
        logging.info("Race is enabled.")
    if len(tags) > 0:
        logging.info("Using build tags: %s", ','.join(tags))

    logging.info("Sending build output to: %s", outdir)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif clean and outdir != '/' and outdir != ".":
        logging.info("Cleaning build directory '%s' before building.", outdir)
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    logging.info("Using version '%s' for build.", version)

    for target, path in targets.items():
        if sys_platform == "windows":
            target += ".exe"

        logging.info("Building target: %s", target)
        build_command = ""

        # Handle static binary output
        if static is True or "static_" in arch:
            if "static_" in arch:
                static = True
                arch = arch.replace("static_", "")
            build_command += "CGO_ENABLED=0 "

        # Handle variations in architecture output
        if arch == "i386" or arch == "i686":
            arch = "386"
        build_command += "GOOS={} GOARCH={} ".format(sys_platform, arch)

        if "arm" in arch and sys_platform != "windows":
            if arch == "armel":
                build_command += "GOARM=5 "
            elif arch == "armhf" or arch == "arm":
                build_command += "GOARM=6 "
            elif arch == "arm64":
                # TODO(rossmcdonald) - Verify this is the correct setting for arm64
                build_command += "GOARM=7 "
            else:
                logging.error("Invalid ARM architecture specified: %s", arch)
                logging.error("Please specify either 'armel', 'armhf', or 'arm64'.")
                return False
        if "arm" in arch:
            build_command += "go build -o {} ".format(os.path.join(outdir, target))
        else:
            build_command += "go build -mod=mod -o {} ".format(os.path.join(outdir, target))
        if race:
            build_command += "-race "
        if len(tags) > 0:
            build_command += "-tags {} ".format(','.join(tags))

        if static:
            build_command += "-a -installsuffix cgo "
            ldflags = "-ldflags=\"-s -X {package}.Version={version} -X {package}.GitBranch={branch} -X {package}.GitCommit={commit}\" "
        else:
            ldflags = "-ldflags=\"-X {package}.Version={version} -X {package}.GitBranch={branch} -X {package}.GitCommit={commit}\" "

        build_command += ldflags.format(
                package = "github.com/openGemini/openGemini/app",
                version = version,
                branch = get_current_branch(),
                commit = get_current_commit())

        build_command += path
        start_time = datetime.utcnow()
        logging.info(build_command)
        run(build_command, shell=True)
        end_time = datetime.utcnow()
        logging.info("Time taken: %ss", (end_time - start_time).total_seconds())
    return True

def write_to_gobuild(content):
    logging.info("write to file")

    if get_system_platform() != "windows":
        with open(gobuild_out, 'w') as f:
            f.write("error\n")
            f.write(content)
    else:
        with open(gobuild_out, 'w') as f:
            f.write("error\n")
            f.write(content.decode("utf-8"))

def main(args):
    global PACKAGE_NAME

    if args.release and args.nightly:
        logging.error("Cannot be both a nightly and a release.")
        return 1

    if args.nightly:
        args.version = increment_minor_version(args.version)
        args.version = "{}~n{}".format(args.version,
                                       datetime.utcnow().strftime("%Y%m%d%H%M"))
        args.iteration = 0

    # Pre-build checks
    check_environ()
    if not check_prereqs():
        return 1
    if args.build_tags is None:
        args.build_tags = []
    else:
        args.build_tags = args.build_tags.split(',')

    orig_commit = get_current_commit(short=True)
    orig_branch = get_current_branch()

    if args.platform not in supported_builds and args.platform != 'all':
        logging.error("Invalid build platform: %s", args.platform)
        return 1

    build_output = {}

    if args.branch != orig_branch and args.commit != orig_commit:
        logging.error("Can only specify one branch or commit to build from.")
        return 1
    elif args.branch != orig_branch:
        logging.info("Moving to git branch: %s", args.branch)
        run("git checkout {}".format(args.branch))
    elif args.commit != orig_commit:
        logging.info("Moving to git commit: %s", args.commit)
        run("git checkout {}".format(args.commit))

    if not args.no_get:
        if not go_get(args.branch, update=args.update, no_uncommitted=args.no_uncommitted):
            return 1

    if args.test:
        if not run_tests(args.race, args.parallel, args.timeout, args.no_vet, args.junit_report):
            return 1

    platforms = []
    single_build = True
    if args.platform == 'all':
        platforms = supported_builds.keys()
        single_build = False
    else:
        platforms = [args.platform]
    for plf in platforms:
        build_output.update( { plf : {} } )
        archs = []
        if args.arch == "all":
            single_build = False
            archs = supported_builds.get(plf)
        else:
            if args.arch not in supported_builds.get(plf):
                logging.error("Invalid build arch: %s", args.arch)
                return 1
            archs = [args.arch]

        for arch in archs:
            od = args.outdir
            if not single_build:
                od = os.path.join(args.outdir, plf, arch)
            if not build(version=args.version,
                         sys_platform=plf,
                         arch=arch,
                         nightly=args.nightly,
                         race=args.race,
                         clean=args.clean,
                         outdir=od,
                         tags=args.build_tags,
                         static=args.static):
                return 1
            build_output.get(plf).update( { arch : od } )

    if orig_branch != get_current_branch():
        logging.info("Moving back to original git branch: %s", orig_branch)
        run("git checkout {}".format(orig_branch))

    return 0


if __name__ == '__main__':
    LOG_LEVEL = logging.INFO
    if '--debug' in sys.argv[1:]:
        LOG_LEVEL = logging.DEBUG
    log_format = '[%(levelname)s] %(funcName)s: %(message)s'
    logging.basicConfig(level=LOG_LEVEL,
                        format=log_format)

    parser = argparse.ArgumentParser(description='OpenGemini build and packaging script.')
    parser.add_argument('--verbose','-v','--debug',
                        action='store_true',
                        help='Use debug output')
    parser.add_argument('--outdir', '-o',
                        metavar='<output directory>',
                        default='./build/',
                        type=os.path.abspath,
                        help='Output directory')
    parser.add_argument('--name', '-n',
                        metavar='<name>',
                        default=PACKAGE_NAME,
                        type=str,
                        help='Name to use for package name (when package is specified)')
    parser.add_argument('--arch',
                        metavar='<amd64|all>',
                        type=str,
                        default=get_system_arch(),
                        help='Target architecture for build output')
    parser.add_argument('--platform',
                        metavar='<linux|all>',
                        type=str,
                        default=get_system_platform(),
                        help='Target platform for build output')
    parser.add_argument('--branch',
                        metavar='<branch>',
                        type=str,
                        default=get_current_branch(),
                        help='Build from a specific branch')
    parser.add_argument('--commit',
                        metavar='<commit>',
                        type=str,
                        default=get_current_commit(short=True),
                        help='Build from a specific commit')
    parser.add_argument('--version',
                        metavar='<version>',
                        type=str,
                        default=get_current_version(),
                        help='Version information to apply to build output (ex: 0.12.0)')
    parser.add_argument('--iteration',
                        metavar='<package iteration>',
                        type=str,
                        default="1",
                        help='Package iteration to apply to build output (defaults to 1)')
    parser.add_argument('--nightly',
                        action='store_true',
                        help='Mark build output as nightly build (will increment the minor version)')
    parser.add_argument('--update',
                        action='store_true',
                        help='Update build dependencies prior to building')
    parser.add_argument('--release',
                        action='store_true',
                        help='Mark build output as release')
    parser.add_argument('--clean',
                        action='store_true',
                        help='Clean output directory before building')
    parser.add_argument('--no-get',
                        action='store_true',
                        help='Do not retrieve pinned dependencies when building')
    parser.add_argument('--no-uncommitted',
                        action='store_true',
                        help='Fail if uncommitted changes exist in the working directory')
    parser.add_argument('--build-tags',
                        metavar='<tags>',
                        help='Optional build tags to use for compilation')
    parser.add_argument('--static',
                        action='store_true',
                        help='Create statically-compiled binary output')
    parser.add_argument('--test',
                        action='store_true',
                        help='Run tests (does not produce build output)')
    parser.add_argument('--junit-report',
                        action='store_true',
                        help='Output tests in the JUnit XML format')
    parser.add_argument('--no-vet',
                        action='store_true',
                        help='Do not run "go vet" when running tests')
    parser.add_argument('--race',
                        action='store_true',
                        help='Enable race flag for build output')
    parser.add_argument('--parallel',
                        metavar='<num threads>',
                        type=int,
                        help='Number of tests to run simultaneously')
    parser.add_argument('--timeout',
                        metavar='<timeout>',
                        type=str,
                        help='Timeout for tests before failing')
    main_args = parser.parse_args()
    print_banner()
    sys.exit(main(main_args))
