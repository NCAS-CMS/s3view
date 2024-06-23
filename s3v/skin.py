from cmd2 import Bg, Fg, ansi

def __style(string, col):
    """ Colour a string with a particular style """
    return ansi.style(string, fg=Fg[col.upper()])

def _i(string, col='green'):
    """ Info string """
    return __style(string,col)
    
def _e(string, col='blue'):
    """ Entity string """
    return __style(string,col)

def _p(string, col='magenta'):
    """ Prompt String """
    return __style(string,col)

def _err(string,col='red'):
    return __style(string,col)

def fmt_size(num, suffix="B"):
    """ Take the sizes and humanize them """
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def fmt_date(adate):
    """ Take the reported date and humanize it"""
    return adate.strftime('%Y-%m-%d %H:%M:%S %Z')
    