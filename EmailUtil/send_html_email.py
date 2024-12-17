import smtplib
import mimetypes
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os


def guess_mime_type(filename):

    # first allow mimetypes to guess type
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type is None:

        # if mimetypes.guess_type() fails, make an educated guess based on the file extension
        ext = os.path.splitext(filename)[1].lower()
        if ext == '.log':
            return 'text/plain'
        # add more mappings as needed
        else:
            return 'application/octet-stream'

    return mime_type


def send(to, subject, body, *attachments):
    """
    Sends an html email with optional attachments.

    Args:
        to (str): Email address of recipient.
        subject (str): Email subject line text.
        body (str|None): Body of the text. This can be either simple text or html formatted text.
        *attachments (str): Strings pointing to attachment file locations. Note: these are not grouped as an array-like
            object. Rather they are listed as individual arguments.
    """

    # get email parameters
    root = os.getenv('market_root')
    email = os.get('email_address')
    sender = os.getenv('email_sender')
    pwd = os.getenv('email_pwd')
    server = os.getenv('email_server')
    port = os.getenv('email_port')

    # set up the SMTP server
    s = smtplib.SMTP(host=server, port=port)
    s.starttls()
    s.login(sender, pwd)

    # create the email
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = to
    msg['Subject'] = subject

    # add the HTML body
    if body is not None and len(body) > 0:
        msg.attach(MIMEText(body, 'html'))

    # add attachments
    for attachment in attachments:

        # get file name
        filename = os.path.basename(attachment)

        # try to guess mime type. the was added because some file types, such as *.log were incorrectly typed and sent
        # as application/octet-stream.
        mime_type = guess_mime_type(filename)

        # continue with best guess at mime type
        main_type, sub_type = mime_type.split('/', 1)
        with open(attachment, 'rb') as file:
            part = MIMEBase(main_type, sub_type)
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
            msg.attach(part)

    # Send the email
    s.send_message(msg)
    s.quit()
