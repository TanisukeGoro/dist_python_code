import time
import sys
# progress bar process
MAX_LEN = 30
def get_progressbar_str(progress):
    """
    Parameters
    ----------
        progress : int
            current times of iteration
    """
    BAR_LEN = int(MAX_LEN * progress)
    return ('[' + '=' * BAR_LEN +
            ('>' if BAR_LEN < MAX_LEN else '') +
            ' ' * (MAX_LEN - BAR_LEN) +
            '] %.1f%%' % (progress * 100.))


def fresh():
    sys.stderr.write('\n')
    sys.stderr.flush()


def progress_bar(curr_progress, end, msg=''):
    """
    Progress Bar function
    progress_bar --> get_progressbar_str --> fresh

    Parameters
    ----------
        curr_progress : int
            current times of iteration
        end : int
            end times of iteration
    """
    progress = 1.0 * curr_progress / end
    sys.stderr.write('\r\033[K' + get_progressbar_str(progress) +"  \033[36mMSG -> " + msg + "\033[0m")
    # sys.stderr.write('\n')
    sys.stderr.flush()

    if(curr_progress >= end):
        sys.stderr.write('\n')

# test progress bar
# for i in range(0, 100):
#     progress_bar(i, 99)
#     time.sleep(0.5)
# ___________________________________
