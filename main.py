import subprocess

import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

def send_email(email, app_name, content, msg_type="info"):
    message = MIMEMultipart()
    message["from"] = "CRM BI<crmbot@homecredit.vn>"
    message["to"] = email
    if msg_type == "error":
        message["subject"] = f"[ERROR] {app_name}"
    else:
        message["subject"] = f"{app_name}"
    html = f"""
        <html>
            <body>
                <xmp>
                    {content}
                </xmp>
            </body>
        </html>
        """
    part = MIMEText(html, "html")
    message.attach(part)
    with smtplib.SMTP(host="smtp.homecredit.vn", port=25) as smtp:
        smtp.ehlo()
        #smtp.starttls()
        smtp.send_message(message)
        
def run_command(command):
    """Executes a command and checks its success."""
    try:
        print(f"============== RUNNING COMMAND: {' '.join(command)} ==============")
        # Run the command and capture output
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
        print(f"SUCCESS")
        print("Output:", result.stdout)
        return True  # Indicate success
    except subprocess.CalledProcessError as e:
        print(f"FAIL!")
        print("Return code:", e.returncode)
        print("Error message:", e.stderr)
        return False  # Indicate failure

commands = [
    ["bash", "submit.sh"],  # Ensure correct path for python
    ['/home/duy.caov/notebooks/apps/vn.bdp.crm/duy_test/duy_demo_venv/bin/python', 'render.py'], 
    ['git', 'add', '.'],
    ["git", "commit", "-m", "testing v1"],
    ["git", "push"]
]

if __name__ == "__main__":
    send_email(
        "duy.caov@homecredit.vn", 
        "STARTING", 
        f"Starting", 
        msg_type="info"
    )   
    
    # Execute each command and check the result
    for cmd in commands:
        success = run_command(cmd)
        if not success:
            print("Stopping execution due to failure.")
            break  
    
    send_email(
        "duy.caov@homecredit.vn", 
        "SUCCEEDED", 
        f"success", 
        msg_type="info"
    )