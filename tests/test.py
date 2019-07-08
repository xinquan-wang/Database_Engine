from src import CSVDataTable
import csv
import json
import logging
import time

logging.basicConfig(level=logging.INFO)

def load(fn):

    result = []
    cols = None
    with open(fn, "r") as infile:
        rdr = csv.DictReader(infile)
        cols = rdr.fieldnames
        for r in rdr:
            result.append(r)
    return result, cols

def test1():
    t = CSVDataTable.CSVDataTable(table_name="Test",
                                  column_names=['foo', 'bar'],
                                  primary_key_columns=['foo'],
                                  loadit=False)
    print("T = ", t)

def test2():
    new_r, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="offices.",
                                  column_names=cols,
                                  primary_key_columns=['officeCode'],
                                  loadit=None)

    t.import_data(new_r)
    print("t = ", t)

def test3():
    i = CSVDataTable.Index(name="Bob", columns=["Last_Name", "First_Name"], kind="UNIQUE")
    r = {"Last_Name": "Ferguson", "First_Name": "Donald", "Uni": "sure"}
    kv = i.compute_key(r)
    print("KV = ", kv)
    i.add_to_index(row=r, rid="2")
    print("I = ", i)

def test4():
    new_r, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/ssol_dummy.csv")
    t = CSVDataTable.CSVDataTable(table_name="ssol_dummy.",
                       column_names=cols,
                       primary_key_columns=['Uni'],
                       loadit=None)
    r = {"Last_Name": "Ferguson", "First_Name": "Donald", "Uni": "sure", "Email": "boo"}
    t.insert(r)
    r["Uni"] = "No"
    t.insert(r)
    print("t = ", t)
    cols = ["Last_Name", "First_Name"]
    t.add_index("Name", cols, "INDEX")

def t1():
    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People",
                                  column_names=cols,
                                  primary_key_columns=['playerID'])

    t.import_data(rows)
    print("T =", t)

    tmp = {"playerID": "willite01", "nameLast": "williams"}

    t.add_index("LASTNAME", ['nameLast', 'nameFirst'], "INDEX")


    start = time.time()
    for i in range(0, 1000):
        r = t.find_by_template(tmp, field_list=None, use_index=True)
        if i == 0:
            print("Row = ", r)
        end = time.time()
        elapsed = end - start
        print("Elapsed time = ", elapsed)

def t3():
    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/Batting.csv")
    t2 = CSVDataTable.CSVDataTable(table_name="Batting",
                                   column_names=cols,
                                   primary_key_columns=['playerID', 'teamID', "yearID", 'stint'])
    t2.import_data(rows)
    print("T = ", t2)
    tmp = {"last_name": "Baggins", "first_name": "Bilbo"}
    print("Specific template = ", t2._get_specific_where(tmp))

def t4():
    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People",
                                  column_names=cols,
                                  primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/Batting.csv")
    t2 = CSVDataTable.CSVDataTable(table_name="Batting",
                                   column_names=cols,
                                   primary_key_columns=['playerID', 'teamID', "yearID", 'stint'])
    t2.import_data(rows)
    print("T = ", t2)

    j = t2.join(t, ['playerID'],
                w_clause={"People.nameLast": "Williams", "People.birthCity": "San Diego"},
                p_clause=['playerID', "People.nameLast", "People.nameFirst", "Batting.teamID",
                          "Batting.yearID", "Batting.stint", "Batting.H", "Batting.AB"], optimize=True)
    print("Result = ", j)
    print("All rows =", json.dumps(j.get_rows(), indent=2))

def t5():

    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People",
                                  column_names=cols,
                                  primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    p = ["playerID", "People.nameLast", "Batting.H"]

    print("Specific template = ", t._get_specific_project(p))

def t6():
    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People",
                                  column_names=cols,
                                  primary_key_columns=['playerID'])

    t.import_data(rows)
    print("T = ", t)
    t.add_index("LASTNAME", ['nameLast'], "INDEX")

    tmp = {"nameLast": "Williams", "birthCity": "San Diego"}


    r = t.find_by_template(tmp, field_list= None, use_index=True)
    print("Answer = ", t)

def t7():
    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="offices",
                                  column_names=cols,
                                  primary_key_columns=['officeCode'])

    t.import_data(rows)
    print("T=", t)
    t.add_index("postalCode", ['postalCode'], "INDEX")

    tmp = {"officeCode": "1", "postalCode": "94080"}

    r = t.find_by_template(tmp, field_list=None, use_index=True)
    idx = t._indexes['postalCode']
    r2 = r._rows
    k = idx.compute_index_value(r2)
    print("Index key is ", k)
    idx.delete_from_index(r2, 2)
    print("Done")

def t8():
    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="offices",
                                  column_names=cols,
                                  primary_key_columns=['officeCode'])

    t.import_data(rows)

    t.save()

def t9():
    rows, cols = load("/Users/wangxinquan/Desktop/2sem/W4111Database/xw2566_HW3/Data/offices.csv")
    t = CSVDataTable.CSVDataTable(table_name="offices",
                                  column_names=cols,
                                  primary_key_columns=['officeCode'])

    t.import_data(rows)

    r = t.delete_by_template({"officeCode": "11"})
    print(r)

test1()
test2()
test3()
test4()
t1()
t3()
t4()
t5()
t6()
t7()
t8()
t9()