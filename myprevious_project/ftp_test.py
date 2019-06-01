import pysftp
import ftplib

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
server='ftp23.meritgroup.com'
username='ocrftpadmin'
password='0(rAdm!N'
directory=''
ftp= ftplib.FTP(server)
ftp.login(username, password)
ftp_dir=directory
# ssh = paramiko.SSHClient()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# srv = pysftp.Connection(host=server, username=username, password=password, port=22,cnopts=cnopts)

# for csvFileName in srv.listdir(directory):
# 	print(csvFileName)

# def sftpExample():
#     cnopts = pysftp.CnOpts()
#     cnopts.hostkeys = None
#     with pysftp.Connection(server, username='me', password='secret', cnopts=cnopts) as sftp:
#         sftp.put('/Users/myusername/projects/order88.csv','/Orders/incoming/myfile.csv')  # upload file to public/ on remote
#         sftp.close()

# sftpExample()