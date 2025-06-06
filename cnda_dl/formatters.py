import logging
import os


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    DARK_GREY = "\033[90m"
    LIGHT_RED = "\033[91m"
    LIGHT_GREEN = "\033[92m"
    LIGHT_YELLOW = "\033[93m"
    LIGHT_BLUE = "\033[94m"
    LIGHT_MAGENTA = "\033[95m"
    LIGHT_CYAN = "\033[96m"
    LIGHT_WHITE = "\033[97m"

    # Background colors
    BACK_BLACK = "\033[40m"
    BACK_RED = "\033[41m"
    BACK_GREEN = "\033[42m"
    BACK_YELLOW = "\033[43m"
    BACK_BLUE = "\033[44m"
    BACK_MAGENTA = "\033[45m"
    BACK_CYAN = "\033[46m"
    BACK_WHITE = "\033[47m"
    BACK_DARK_GREY = "\033[100m"
    BACK_LIGHT_RED = "\033[101m"
    BACK_LIGHT_GREEN = "\033[102m"
    BACK_LIGHT_YELLOW = "\033[103m"
    BACK_LIGHT_BLUE = "\033[104m"
    BACK_LIGHT_MAGENTA = "\033[105m"
    BACK_LIGHT_CYAN = "\033[106m"
    BACK_LIGHT_WHITE = "\033[107m"


class ParensOnRightFormatter1(logging.Formatter):
    def format(self, record):
        log_message = f"{record.msg}"
        log_level = f"{record.levelname}"
        func_name = (f"{record.funcName}")
        if func_name[-1] == '.':
            func_name[-1] = f"{Colors.DARK_GREY}.{Colors.RESET}"
        # Determine total width of terminal window
        terminal_width = os.get_terminal_size()[0]
        # Calculate the right margin position for the log level and function name
        if log_level == "INFO":
            right_margin_text = f"{Colors.LIGHT_GREEN}({log_level}, {func_name}){Colors.RESET}"
        elif log_level == "WARNING":
            right_margin_text = f"{Colors.YELLOW}({log_level}, {func_name}){Colors.RESET}"
        elif log_level == "ERROR":
            right_margin_text = f"{Colors.RED}({log_level}, {func_name}){Colors.RESET}"
        necessary_padding = terminal_width - len(log_message) - len(right_margin_text)
        # Ensure padding is non-negative
        padding = f'{Colors.DARK_GREY}.{Colors.RESET}' * max(0, necessary_padding)
        return f"{log_message}{padding}{right_margin_text}"
