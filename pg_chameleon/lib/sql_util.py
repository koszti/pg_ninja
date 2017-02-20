import re
import sys
class sql_token:
	"""
	Class sql_token. Tokenise the sql statements captured by the mysql replication.
	Each statement is converted in a python dictionary being used by pg_engine.
	"""
	def __init__(self):
		self.tokenised=[]
		
		#regexp for query type
		self.m_create_table=re.compile(r'(CREATE\s*TABLE)\s*(?:IF\s*NOT\s*EXISTS)?\s*(?:`)?(\w*)(?:`)?', re.IGNORECASE)
		self.m_drop_table=re.compile(r'(DROP\s*TABLE)\s*(?:IF\s*EXISTS)?\s*(?:`)?(\w*)(?:`)?', re.IGNORECASE)
		self.m_alter_table=re.compile(r'(?:(ALTER\s+?TABLE)\s+(`?\b.*?\b`?))\s+((?:ADD|DROP)\s+(?!UNIQUE INDEX)(?:COLUMN)?.*,?)', re.IGNORECASE)
		self.m_alter_index=re.compile(r'(?:(ALTER\s+?TABLE)\s+(`?\b.*?\b`?))\s+((?:ADD|DROP)\s+(?:UNIQUE)?\s*?(?:INDEX).*,?)', re.IGNORECASE)
		self.m_alter=re.compile(r'((?:(?:ADD|DROP)\s+(?:COLUMN)?))(.*?,)', re.IGNORECASE)
		
	def capture_alter(self,  alter_string):
		capture_allowed=['ADD COLUMN', 'DROP COLUMN', 'ADD', 'DROP']
		capture_lst=[]
		captured=self.m_alter.findall(alter_string)
		for alter in captured:
			capture_dic={}
			capture_dic["command"]=alter[0].upper().strip()
			if capture_dic["command"]in capture_allowed:
				column=alter[1].split()
				capture_dic["column"]=column[0].strip().strip(',').strip('`')
				capture_lst.append(capture_dic)
		return capture_lst
	def capture_ddl(self, sql_string):
		"""
			Splits the sql string in statements using the conventional end of statement marker ;
			A regular expression greps the words and parentesis and a split converts them in
			a list. Each list of words is then stored in the list token_list.
			
			:param sql_string: The sql string with the sql statements.
		"""
		
		statements=sql_string.split(';')
		for statement in statements:
			stat_dic={}
			stat_cleanup=statement.replace('\n', ' ')
			stat_cleanup=re.sub(r'/\*.*?\*/', '', stat_cleanup, re.DOTALL)
			stat_cleanup=re.sub(r'--.*?\n', '', stat_cleanup)
			stat_cleanup=re.sub(r'[\b)\b]', ' ) ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b(\b]', ' ( ', stat_cleanup)
			stat_cleanup=re.sub(r'[\b,\b]', ', ', stat_cleanup)
			stat_cleanup=re.sub(r'\n*', '', stat_cleanup)
			stat_cleanup=re.sub("\([\w*\s*]\)", " ",  stat_cleanup)
			stat_cleanup=stat_cleanup.strip()
			mcreate_table=self.m_create_table.match(stat_cleanup)
			mdrop_table=self.m_drop_table.match(stat_cleanup)
			malter_table=self.m_alter_table.match(stat_cleanup)
			malter_index=self.m_alter_index.match(stat_cleanup)
			if mcreate_table:
				command=' '.join(mcreate_table.group(1).split()).upper().strip()
				stat_dic["command"]=command
				stat_dic["name"]=mcreate_table.group(2).replace('`', '')
			elif mdrop_table:
				command=' '.join(mdrop_table.group(1).split()).upper().strip()
				stat_dic["command"]=command
				stat_dic["name"]=mdrop_table.group(2).replace('`', '')
			elif malter_index:
				pass
			elif malter_table:
				stat_dic["command"]=malter_table.group(1).upper().strip()
				stat_dic["name"]=malter_table.group(2).replace('`', '')
				alter_string=malter_table.group(3)+','
				stat_dic["alter"]=self.capture_alter(alter_string)
				
			self.tokenised.append(stat_dic)
		
