import sqlite3
from sqlite3 import Error
from random import randrange

# Նշեք այստեղ ձեր բազայի հասցեն (C:\Users\YourUserName\Documents\myDbName.db)
DATABASE = "/Users/voskan/Desktop/qa/qa.db"

# EmployeeDetails աղյուսակի ստեղծման sql հարցման կոդը
sql_create_EmployeeDetails_table = ''' CREATE TABLE IF NOT EXISTS `EmployeeDetails` (
    `Empid` INTEGER PRIMARY KEY AUTOINCREMENT,
    `FullName` VARCHAR(150) NOT NULL,
    `ManagerId` INTEGER NOT NULL,
    `DateOfJoining` DATE DEFAULT current_timestamp
) '''

# EmployeeSalary աղյուսակի ստեղծման sql հարցման կոդը
sql_create_EmployeeSalary_table = ''' CREATE TABLE IF NOT EXISTS `EmployeeSalary` (
    `Slid` INTEGER PRIMARY KEY AUTOINCREMENT,
    `Empid` INTEGER NOT NULL,
    `Project` VARCHAR(10) NOT NULL,
    `Salary` INTEGER,
    FOREIGN KEY(Empid) REFERENCES EmployeeDetails(Empid)  
) '''

# Test աղյուսակի ստեղծման sql հարցման կոդը
sql_create_Test_table = ''' CREATE TABLE IF NOT EXISTS `Test` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `title` VARCHAR(500) NOT NULL,
    `email` VARCHAR(100) NOT NULL UNIQUE,
    `age` TINYINT(2) NOT NULL
) '''

# Test աղյուսակի ջնջման sql հարցման կոդը
sql_delete_Test_table = ''' DROP TABLE IF EXISTS `Test` '''

# պատրաստում ենք EmployeeSalary աղյուսակի տվյալները
def data_for_insert_EmployeeSalary_table(id, manager, project, salary):
    data = {
        "Empid": id,
        "Project": project,
        "Salary": salary
    }

    return (data.get('Empid'), data.get('Project'), data.get('Salary'))

# Ստեղծում ենք բազայի հետ connection և հետվերադարձնում connect-ի հղումը
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

# կատարում ենք sql հարցումներ
def sql_execute(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

# Ավելացնում ենք EmployeeDetails աղյուսակում տվյալները և հետ վերադարձնում id
def insert_EmployeeDetails_data(conn, data):
    sql = ''' INSERT INTO `EmployeeDetails`(FullName, ManagerId)
                VALUES(?,?) '''
    
    c = conn.cursor()
    c.execute(sql, data)
    conn.commit()
    return c.lastrowid

# Ավելացնում ենք EmployeeSalary աղյուսակում տվյալները և հետ վերադարձնում id
def insert_EmployeeSalary_data(conn, data):
    sql = ''' INSERT INTO `EmployeeSalary`(Empid, Project, Salary)
                VALUES(?,?,?) '''
    
    c = conn.cursor()
    c.execute(sql, data)
    conn.commit()
    return c.lastrowid

# Ստանում ենք EmployeeDetails աղյուսակի բոլոր տվյալները
def select_all_EmployeeDetails_data(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM EmployeeDetails")
    
    rows = c.fetchall()

    for row in rows:
        print(row)

# Ստանում ենք EmployeeSalary աղյուսակի բոլոր տվյալները
def select_all_EmployeeSalary_data(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM EmployeeSalary")
    
    rows = c.fetchall()

    for row in rows:
        print(row)

# Ստանում ենք P1 պրոեկտում աշխատողների քանակը
def select_p1_employees_users_count(conn):
    c = conn.cursor()
    c.execute(''' SELECT count(*) FROM `EmployeeDetails`
        JOIN `EmployeeSalary` ON `EmployeeDetails`.`Empid`=`EmployeeSalary`.`Empid`
        WHERE `EmployeeSalary`.`Project`="P1" ''')
    
    count_data = c.fetchone()
    print(count_data)

# Ստանում ենք բոլոր օգտվողներին ըստ ստացած գումարի
def select_employees_by_salary(conn):
    c = conn.cursor()
    c.execute(''' SELECT `FullName` FROM `EmployeeDetails`
        JOIN `EmployeeSalary` ON `EmployeeDetails`.`Empid`=`EmployeeSalary`.`Empid`
        WHERE `EmployeeSalary`.`Salary` >= 5000 AND `EmployeeSalary`.`Salary` <= 10000 ''')
    
    rows = c.fetchall()

    for row in rows:
        print(row)

# Դասակարգում ենք ըստ պրոեկտի քանակի
def select_sort_by_projects_count(conn):
    c = conn.cursor()
    c.execute(''' SELECT `ManagerId` FROM `EmployeeDetails` ORDER BY `ManagerId` DESC ''')
    
    rows = c.fetchall()

    for row in rows:
        print(row)

# Ստանում ենք անունը և աշխատավարձի չափը
def select_name_and_salary(conn):
    c = conn.cursor()
    c.execute(''' SELECT FullName, Salary FROM `EmployeeDetails`
        JOIN `EmployeeSalary` ON `EmployeeDetails`.`Empid`=`EmployeeSalary`.`Empid` ''')
    
    rows = c.fetchall()

    for row in rows:
        print(row)

# Ստանում ենք ըստ տարեթվի
def select_by_date(conn):
    c = conn.cursor()
    c.execute(''' SELECT * FROM `EmployeeDetails` WHERE `DateOfJoining` >= date('2016') ''')
    
    rows = c.fetchall()

    for row in rows:
        print(row)

# ՍRight join
def select_and_righ_join(conn):
    c = conn.cursor()
    c.execute(''' SELECT * FROM `EmployeeDetails`
        JOIN `EmployeeSalary` ON `EmployeeDetails`.`Empid`=`EmployeeSalary`.`Empid` ''')
    
    rows = c.fetchall()

    for row in rows:
        print(row)

def main():
    conn = create_connection(DATABASE)

    if conn is not None:
        # Ստեղծում ենք երկու աղյուսակները
        sql_execute(conn, sql_create_EmployeeDetails_table)
        sql_execute(conn, sql_create_EmployeeSalary_table)
        sql_execute(conn, sql_create_Test_table)

        # ջնջում Test ենք աղյուսակը
        sql_execute(conn, sql_delete_Test_table)

        # ավելացնում ենք EmployeeDetails աղյուսակում տվյալները
        # Empid1 = insert_EmployeeDetails_data(conn, ("John Snow", randrange(999)))
        # Empid2 = insert_EmployeeDetails_data(conn, ("Walter White", randrange(999)))
        # Empid3 = insert_EmployeeDetails_data(conn, ("test Name", randrange(999)))

        # # ավելացնում ենք EmployeeSalary աղյուսակում տվյալները
        # insert_EmployeeSalary_data(conn, (Empid2, "P1", randrange(99999)))
        # insert_EmployeeSalary_data(conn, (Empid1, "P2", randrange(99999)))
        # insert_EmployeeSalary_data(conn, (3, "P1", randrange(99999)))

        # Ստանում ենք EmployeeDetails աղյուսակի բոլոր տվյալները
        # select_all_EmployeeDetails_data(conn)

        # Ստանում ենք EmployeeSalary աղյուսակի բոլոր տվյալները
        # select_all_EmployeeSalary_data(conn)

        # ՏՆԱՅԻՆԻ SELECT-ները ԸՍՏ ՀԵԵՎԹԱԿԱՆՈՒԹՅԱՄԲ
        # select_p1_employees_users_count(conn)
        # select_employees_by_salary(conn)
        # select_sort_by_projects_count(conn)
        # select_name_and_salary(conn)

        # select_by_date(conn)

        select_and_righ_join(conn)

if __name__ == "__main__":
    main()