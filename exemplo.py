import psycopg2

class Banco:
	def __init__(self,host, database, user, password):
    	self.host = host
    	self.database = database
    	self.user = user
    	self.password = password

    	self.banco = self.conectarBanco(host,database,user,password)

	def conectarBanco(self):
		try:
			conn = psycopg2.connect(self.host,self.database,self.user,self.password)
			return conn
		except Exception as e:
			print ("Não foi possível conectar ao banco de dados.")
			print (e)

	#Métodos de inserção
	def inserirDepartamento(self,nome,codigo,gerente,iniciogerente):
		self.inserir("departamento", "nome,codigo,gerente, iniciogerente","'"+nome+"',"+codigo+",'"+gerente+"','"+iniciogerente+"'")

	def inserirDependente(self,empregado, nome, sexo, dtnascimento, parentesco):
		self.inserir("dependente", "empregado,nome,sexo, dtnascimento,parentesco","'"+empregado+"','"+nome+"','"+sexo+"','"+"','"+dtnascimento+"','"+"','"+parentesco+"'")

	def inserirEmpregado(self,nome ,nomedomeio, sobrenome, codigo , dtnascimento, endereco , sexo , salario, gerente, departamento ):
		self.inserir("empregado", "nome,nomedomeio,sobrenome,codigo,dtnascimento,endereco,sexo,salario,gerente,departamento","'"+nome+"','"+nomedomeio+"','"+sobrenome+"'"+"',"+codigo+",'"+dtnascimento+"','"+endereco+"','"+sexo+"',"+salario+",'"+gerente+"',"+departamento)	
	
	def inserirLocal(self,departamento,nome):
		self.inserir("local", "departamento,nome", departamento+",'"+nome+"'")
	
	def inserirProjeto(self,descricao,codigo,local,departamento):
		self.inserir("projeto", "descricao,codigo,local,departamento", "'"+descricao+"',"+codigo+",'"+local"',"+departamento)	

	def inserirTrabalhaem(self,empregado,projeto,horas):
		self.inserir("projeto", "empregado,projeto,horas", "'"+empregado+"',"+projeto+","+horas)	


	def inserir(self,tabela,campos,dados):
		try:
			cursor = self.banco.cursor()

			postgres_insert_query = """ INSERT INTO """+tabela+""" ("""+campos+""") VALUES ("""+dados+""")"""
			cursor.execute(postgres_insert_query)
			self.banco.commit()
			if (cursor.rowcount):
				print ("Registro inserido com sucesso em "+tabela+".")

			cursor.close()

		except Exception as e:
			print ("Erro ao inserir: ", e)
		

	#métodos de remoção
	def removerDepartamento(self,codigo):
		self.remover("departamento", "codigo",codigo)

	#sobrecarga, pois tem duas chaves primarias
	def removerDependente(self,empregado):
		self.remover("dependente", "empregado",empregado)	

	def removerDependente(self,nome):
		self.remover("dependente", "nome",nome)
			
	def removerEmpregado(self,codigo):
		self.remover("empregado", "codigo",codigo)

	#sobrecarga, pois tem duas chaves primarias
	def removerLocal(self,departamento):
		self.remover("local", "departamento",departamento)	

	def removerLocal(self,nome):
		self.remover("local", "nome",nome)
	
	def removerProjeto(self,codigo):
		self.remover("projeto", "codigo",codigo)

	#sobrecarga, pois tem duas chaves primarias
	def removerTrabalhaem(self,empregado):
		self.remover("trabalhaem", "empregado",empregado)	
			
	def removerTrabalhaem(self,projeto):
		self.remover("trabalhaem", "projeto",projeto)		
		

	def remover(self,tabela,primarykey,valor):
		try:
			cursor = self.banco.cursor()

			postgres_insert_query = """ DELETE FROM """+tabela+""" WHERE """primarykey""" = """+valor+""""""
			cursor.execute(postgres_insert_query)
			self.banco.commit()
			if (cursor.rowcount):
		   		print ("Registro excluído com sucesso de "+tabela+".")
				
			cursor.close()

		except Exception as e:
			print ("Erro ao remover:", e)

	def listarDepartamentos(self,banco):
		try:
			cursor = self.banco.cursor()

			sql = 'SELECT d.nome, l.nome FROM departamento d LEFT JOIN local l ON d.codigo = l.departamento'

			cursor.execute(sql)
			resultado = cursor.fetchall()
			for tupla in resultado:
				print (tupla[0], " - ", tupla[1])

			cursor.close()

		except Exception as e:
			print ("Deu ruim mano: ", e)
		
	try:
		conexao = conectarBanco()

		inserirDepartamento(conexao)
		listarDepartamentos(conexao)
		removerDepartamento(conexao)

	finally:
	    if(conexao):
	        conexao.close()

banco = Banco('localhost','prova','postgres','postgres')