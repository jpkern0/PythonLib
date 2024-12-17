import os
import send_html_email
from bs4 import BeautifulSoup
from colorama import Fore


def send(to_addr=None, msg='N/A', stack='N/A', progress='N/A', subject=None, *attachments):

    """
    Prints a message and sends an email containing a formatted error message.

    Args:
        to_addr (str): Email address to send message to.
        msg (Exception): Error message to be sent.
        stack (str): Contents of the error stack.
        progress (str): Processing progress.
        subject (str|None): Alternate subject. If None, a default error subject is used.
        *attachments (str): Zero or more file paths for attachments.
    """

    # default mail address
    if to_addr is None:
        to_addr = os.getenv('email_address')
    
    # Parse stack
    lines = stack.split('\n')

    # Remove from stack lines with long strings of '^'
    lines = [s for s in lines if '^^' not in s]

    # Only keep even lines of stack
    if len(lines) > 2:
        lines = [lines for i, lines in enumerate(lines) if i % 2 == 1]

    # Add stack header, remove any empty lines, lead spaces and remove the last line which is a repeat of msg
    lines = [s for s in lines if s != ""]
    lines = [s.replace('  File', 'File') for s in lines]

    # Some files are in angle brackets which confuse html. Change these to print properly
    lines = [s.replace('<', '&lt;') for s in lines]
    lines = [s.replace('>', '&gt;') for s in lines]

    # Make each line a stack a separate html row
    stack_rows = ['<tr><td>Stack &darr;&darr;: </td><td>{}</td></tr>'.format(lines[0])] + \
                 ['<tr><td></td><td>{}</td></tr>'.format(s) for s in lines[1:]]

    # Format the rest of the email message
    progress_row = '<tr><td style="padding-right: 10px;">Progress:  </td><td>{}</td></tr>'.format(progress)
    error_row = '<tr><td>Error: </td><td>{}</td></tr>'.format(msg)
    msg1 = ''.join(stack_rows)
    msg2 = progress_row
    msg3 = error_row
    msg = msg1 + msg2 + msg3
    body = '''<!DOCTYPE html><html><body><table style="font-weight: bold;">{}</table></body></html>'''.format(msg)

    # Print to console
    print(Fore.CYAN + '\n')
    print('-------------------------------------Start Error Text-----------------------------------')
    soup = BeautifulSoup(body, 'html.parser')
    table = soup.find('table')
    for row in table.find_all('tr'):
        cols = [col.text for col in row.find_all('td')]
        print('{:<11}{:<}'.format(*cols))
    print('--------------------------------------End Error Text-------------------------------------')
    print(Fore.WHITE)

    # Send email
    subject = 'Program Error Notification' if subject is None else subject
    send_html_email.send(to_addr, subject, body, *attachments)


# main entry point
if __name__ == '__main__':
    send(msg='N/A', stack='N/A', progress='N/A')
