"""создание JSON пакета с инофрмацией по студентам с выбранными ИД.
"""
import json
from psycopg2 import Error
import psycopg2

try:
	# Подключение к существующей базе данных
	connection = psycopg2.connect(user="myprojectuser",
								  # пароль, который указали при установке PostgreSQL
								  password="password",
								  host="localhost",
								  port="5432",
								  database="myproject")
	# Курсор для выполнения операций с базой данных

	cursor = connection.cursor()
	"""
	insert_query = '''INSERT INTO status 
	VALUES (4, '52177e21-20a1-4ca1-9989-4da96da65239', 'student', 'new'),
			(5, '8d4686f4-7c7a-4adf-9bef-3cf0e8d6cd80', 'student', 'new')
	'''
	cursor.execute(insert_query)
	connection.commit()
	


	select_query = 'select s."ID" , s."HumanID" , h."HumanFirstName" , h."HumanMiddleName" , h."HumanLastName" , h."HumanSNILS" , h."HumanINN" , h."HumanBasicEmail" from student s join human h on h."ID"  = s."HumanID";'
	cursor.execute(select_query)
	mobile_records_one = cursor.fetchone()
	print("Вывод первой записи", mobile_records_one)

	create_table_query = '''CREATE TABLE status (
		id  serial PRIMARY KEY,
		external_id varchar (50) NOT NULL,
		table_name varchar (50) NOT NULL,
		status varchar (50) NOT NULL);'''
	cursor.execute(create_table_query)
	connection.commit()"""



	def get_dict_with_new_student():
		# функция, которая формирует строку со значениями - ид со статусом new из таблицы студенты
		select_query = '''select * from status where "status" = 'new' and "table_name" = 'student' '''
		cursor.execute(select_query)
		select = cursor.fetchall()
		list_of_id = list()
		for row in select:
			# print("ID =", row[0])
			# print("external_id =", row[1])
			# print("table_name =", row[2])
			# print("status =", row[3])
			list_of_id.append(row[1])
		id_param = "'" + "','".join(list_of_id) + "'"
		return id_param 								# тип параметра - строка

	def get_dict_from_list(one_student_information):  # функция для преобразования словаря в JSON объект
		list_of_keys = ('external_id', 'surname', 'name', 'middle_name', 'snils', 'inn', 'email')
		stud_dict = dict(zip(list_of_keys, one_student_information))
		# преобразование в JSON в аргументом для неломки кодировки
		return stud_dict  # json.dumps(stud_dict, ensure_ascii=False)


	def get_student_info_list():  # Функция, которая создает пакет (список) по всем студентам
		id_param = get_dict_with_new_student()
		#print(id_param)
		select_query = '''select s."ID" , h."HumanLastName", h."HumanFirstName" ,h."HumanMiddleName" ,
		h."HumanSNILS", h."HumanINN", h."HumanBasicEmail"
		from student s join human h on h."ID"  = s."HumanID"'''
		# where s."ID" = '001c8f10-0604-4693-b584-6ad9e89a39fd';
		cursor.execute(select_query)
		student_id = cursor.fetchall()
		all_students_information = list()
		for row in student_id:  # Список для сохранения информации по одному студенту
			one_student_information = list()
			for i in range(7):  # пробежка по столбцам в одной строке - инфа по одному студенту
				one_student_information.append(row[i])
			one_student_dict = get_dict_from_list(one_student_information)
			all_students_information.append(one_student_dict)
		ready_requst = [{'organization_id': '08hksdj20349hqo258035hk'}]
		print(ready_requst)
		ready_requst.append(all_students_information)
		print("I try:", ready_requst)
		with open('students.json', 'w') as fp:
			json.dump(ready_requst, fp, ensure_ascii=False)
		return all_students_information, json.dumps(ready_requst, ensure_ascii=False)

	# print(get_dict_with_new())
	# list_of_student_id.append(student_id)
	print(get_student_info_list())
	# json_data = json.dump()
	# print(json_data)
	# print(get_dict_with_new())


except (Exception, Error) as error:
	print("Ошибка при работе с PostgreSQL", error)
finally:
	if connection:
		cursor.close()
		connection.close()
		print("Соединение с PostgreSQL закрыто")
