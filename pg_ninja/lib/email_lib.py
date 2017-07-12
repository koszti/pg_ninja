import smtplib
class email_alerts:
	def __init__(self, email_config, logger):
		self.email_config=email_config
		self.server=None
		self.logger=logger
	
	def connect_to_smtp(self):
		self.logger.info("trying to connect to SMTP server")
		try:
			self.server = smtplib.SMTP(self.email_config["smtp_server"], self.email_config["smtp_port"])
			if self.email_config["smtp_tls"]:
				self.server.starttls()
			if self.email_config["smtp_login"]:
				self.server.login(self.email_config["smtp_username"], self.email_config["smtp_password"])
			
		except:
			self.logger.error("could not send the email")
			self.server=None
	
		
	def disconnect_fm_smtp(self):
		if self.server:
			self.logger.info("disconnecting from SMTP server")
			self.server.quit()
		
	
	def send_start_replica_email(self):
		self.connect_to_smtp()
		if self.server:
			msg_subject="[%s] - replica started" % self.email_config["subj_prefix"]
			msg_body="""%s the  database replica  just started. If this is expected please ignore the email. Otherwise check the replication system is working properly. """
			msg_header=	"From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.email_config["email_from"],  ', '.join(self.email_config["email_rcpt"]), msg_subject)
			msg_body=msg_body	% (msg_header)
			
			self.server.sendmail(self.email_config["email_from"], self.email_config["email_rcpt"], msg_body)
			self.disconnect_fm_smtp()
	
	def send_end_init_replica(self):
		self.connect_to_smtp()
		if self.server:
			msg_subject="[%s] - init replica complete" % self.email_config["subj_prefix"]
			msg_body="""%s the  database replica  initialisation is now complete. Please start the replica process. """
			msg_header=	"From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.email_config["email_from"],  ', '.join(self.email_config["email_rcpt"]), msg_subject)
			msg_body=msg_body	% (msg_header)
			
			self.server.sendmail(self.email_config["email_from"], self.email_config["email_rcpt"], msg_body)
			self.disconnect_fm_smtp()

	def send_end_sync_replica(self):
		self.connect_to_smtp()
		if self.server:
			msg_subject="[%s] - sync replica complete" % self.email_config["subj_prefix"]
			msg_body="""%s the  database replica  synchronisation is now complete. Please start the replica process. """
			msg_header=	"From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.email_config["email_from"],  ', '.join(self.email_config["email_rcpt"]), msg_subject)
			msg_body=msg_body	% (msg_header)
			
			self.server.sendmail(self.email_config["email_from"], self.email_config["email_rcpt"], msg_body)
			self.disconnect_fm_smtp()
	def send_end_sync_obfuscation(self):
		self.connect_to_smtp()
		if self.server:
			msg_subject="[%s] - sync obfuscation complete" % self.email_config["subj_prefix"]
			msg_body="""%s the  obfuscated schema synchronisation is now complete. Please start the replica process. """
			msg_header=	"From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.email_config["email_from"],  ', '.join(self.email_config["email_rcpt"]), msg_subject)
			msg_body=msg_body	% (msg_header)
			
			self.server.sendmail(self.email_config["email_from"], self.email_config["email_rcpt"], msg_body)
			self.disconnect_fm_smtp()
