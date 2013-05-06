import os
import sys
import re
import subprocess


# Executing the interpreter in a subprocess
def _assert_python(expected_success, *args, **env_vars):
    cmd_line = [sys.executable]
    if not env_vars:
        cmd_line.append('-E')
    # Need to preserve the original environment, for in-place testing of
    # shared library builds.
    env = os.environ.copy()
    # But a special flag that can be set to override -- in this case, the
    # caller is responsible to pass the full environment.
    if env_vars.pop('__cleanenv', None):
        env = {}
    env.update(env_vars)
    cmd_line.extend(args)
    p = subprocess.Popen(cmd_line, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env=env)
    try:
        out, err = p.communicate()
    finally:
        subprocess._cleanup()
        p.stdout.close()
        p.stderr.close()
    rc = p.returncode
    err = strip_python_stderr(err)
    if (rc and expected_success) or (not rc and not expected_success):
        raise AssertionError(
            "Process return code is %d, "
            "stderr follows:\n%s" % (rc, err.decode('ascii', 'ignore')))
    return rc, out, err


def assert_python_ok(*args, **env_vars):
    """
    Assert that running the interpreter with `args` and optional environment
    variables `env_vars` is ok and return a (return code, stdout, stderr)
    tuple.
    """
    return _assert_python(True, *args, **env_vars)


def strip_python_stderr(stderr):
    """Strip the stderr of a Python process from potential debug output emitted
    by the interpreter.

    This will typically be run on the result of the communicate() method of a
    subprocess.Popen object.
    """
    stderr = re.sub(br"\[\d+ refs\]\r?\n?", b"", stderr).strip()
    return stderr
