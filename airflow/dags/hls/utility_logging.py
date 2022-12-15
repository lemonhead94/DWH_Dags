import logging
import sys
import traceback


def __extract_function_name() -> str:
    """Extracts failing function name from Traceback object."""
    tb = sys.exc_info()[-1]
    stk = traceback.extract_tb(tb, 1)
    fname = stk[0][3]
    return str(fname)


def setup_logging_to_file(filename: str) -> None:
    """Setup logging to log to a file."""
    logging.basicConfig(
        filename=filename,
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def log_exception(e: Exception) -> None:
    """Logs exception with traceback."""
    logging.error(
        "Function {function_name} raised ({exception_docstring}): {exception_message}".format(
            function_name=__extract_function_name(),  # this is optional
            exception_docstring=e.doc,  # type: ignore
            exception_message=str(e),
        )
    )
