import json
import csv
import logging
import copy
import pandas

class Index():

    def __init__(self, index_name=None, table=None, index_columns=None, kind=None, loadit=None):

        if loadit:
            logging.debug("Loading an index.")
            self.from_json(table, loadit)
            logging.debug("Loaded index. name = %s, columns = %s, kind = %s, table = %s",
                          self._index_name, str(self._columns), self._kind, self._table.get_table_name())
        else:
            logging.debug("Creating index. name = %s, columns = %s, kind = %s, table = %s",
                          index_name, str(index_columns), kind, table.get_table_name)
            index_columns.sort()
            self._index_name = index_name
            self._columns = index_columns
            self._kind = kind
            self._table = table
            self._table_name = table.get_table_name()
            self._index_data = None

        if self._columns is not None:
            self._columns.sort()

    def __str__(self):

        result = ""
        result += "Index name: " + self._index_name
        result += "\nKind: " + self._kind
        result += "\nIndex columns: " + str(self._columns)
        result += "\nTable name: " + self._table_name

        if self._index_data is not None:
            keys = list(self._index_data.keys())
            result += "\nNo. of unique index values: " + str(len(keys))
            cnt = min(5, len(keys))
            result += "Entries:\n"
            for i in range(0, cnt):
                result += "[" + keys[i] + ": " + json.dumps(self._index_data[keys[i]], indent=2) + "]\n"

        return result

    def compute_key(self, row):

        key_v = [row[k] for k in self._columns]
        key_v = "_".join(key_v)
        return key_v

    def add_to_index(self, row, rid):

        if self._index_data is None:
            self._index_data = {}
        key = self.compute_key(row)
        bucket = self._index_data.get(key, [])
        if self._kind != "INDEX":
            if len(bucket) > 0:
                raise KeyError("Duplicate key")
        bucket.append(rid)
        self._index_data[key] = bucket

    def delete_from_index(self, row, rid):

        index_key = self.compute_key(row)

        bucket = self._index_data.get(index_key, None)

        if bucket is not None:
            bucket.remove(rid)

            if len(bucket) == 0:
                del(self._index_data[index_key])

    def to_json(self):

        result = {}
        result["name"] = self._index_name
        result["columns"] = self._columns
        result["kind"] = self._kind
        result["table_name"] = self._table_name
        result["index_data"] = self._index_data
        return result

    def from_json(self, table, loadit):
        if loadit:
            result = table.load()
            return result

    def matches_index(self, template):

        k = set(list(template.keys()))
        c = set(self._columns)

        if c.issubset(k):
            if self._index_data is not None:
                kk = len(self._index_data.keys())
            else:
                kk = 0
        else:
            kk = None

        return kk

    def find_rows(self, tmp):

        t_keys = tmp.keys()
        t_vals = [tmp[k] for k in self._columns]
        t_s = "_".join(t_vals)

        d = self._index_data.get(t_s, None)
        if d is not None:
            d = list(d.keys())

        return d

    def get_no_of_entries(self):
        return len(list(self._index_data.keys()))

class CSVDataTable():

    _default_directory = "../../DB/"

    def __init__(self, table_name, column_names=None, primary_key_columns=None, loadit=False):

        self._table_name = table_name
        self._primary_key_columns = primary_key_columns
        self._column_names = column_names

        self._indexes = None

        if not loadit:
            if column_names is None or table_name is None:
                raise ValueError("Did not provide table_name for column_names for table create.")

            self._next_row_id = 1

            self._rows = {}

            if primary_key_columns:
                self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")

    def get_table_name(self):
        return self._table_name

    def build(self, i_name):
        idx = self._indexes[i_name]
        for k, v in self._rows.items():
            idx.add_to_index(v, k)

    def add_index(self, index_name, column_list, kind):

        if self._indexes is None:
            self._indexes = {}

        self._indexes[index_name] = Index(index_name=index_name, index_columns=column_list, kind=kind)
        self.build(index_name)

    def __str__(self):
        result = "Table name = " + self._table_name
        result += "\nColumn name: " + str(self._column_names)
        result += "\nPrimary key columns: " + str(self._primary_key_columns)
        result += "\nNext row id: " + str(self._next_row_id)

        if self._rows is not None:

            rids = self._rows.keys()
            rs = list(self._rows.values())
            no_rows = str(len(rids))

            result += "\nNo. of rows: " + no_rows

            cnt = min(len(rids), 5)
            if cnt > 0:
                result += "\nFirst " + str(cnt) + " rows:\n"
                tmp = pandas.DataFrame.from_dict(self.get_rows_with_rids(), orient="index")
                tmp = tmp.head(5)
                result += tmp.to_string()

            else:
                result += "\nNo. of rows: 0"

            if self._indexes:
                result += "\nIndexes:\n"
                for k, v in self._indexes.items():
                    result += str(v)

            return result

        return result

    def _remove_rows(self, rid):

        r = self._rows[rid]
        for n, idx in self._indexes.items():
            idx.delete_from_index(r, rid)

        del[self._rows[rid]]

    def get_rows(self):

        if self._rows is not None:
            result = []
            for k, v in self._rows.items():
                result.append(v)
        else:
            result = None
        return result

    def get_rows_with_rids(self):

        return self._rows

    def _get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id

    def get_best_index(self, t):

        best = None
        n = None

        if self._indexes is not None:
            for k, v in self._indexes.items():
                cnt = v.matches_index(t)
                if cnt is not None:
                    if best is None:
                        best = cnt
                        n = k
                    else:
                        if cnt > best:
                            best = v.get_no_of_entries()
                            n = k

        return n

    def matches_template(self, row, tmp):
        result = False

        if tmp is None or tmp == {}:
            result = True
        else:
            for k in tmp.keys():
                v = row.get(k, None)
                if v!= tmp[k]:
                    result = False
                    break
            else:
                result = True

        return result

    def get_index_and_selectivity(self, cols):

        on_template = dict(zip(cols, [None] * len(cols)))
        best = None
        n = self.get_best_index(on_template)

        if n is not None:
            best = len(list(self._rows.keys())) / (self._indexes[n].get_no_of_entries())

        return n, best

    def find_by_index(self, tmp, idx):

        r = idx.find_rows(tmp)
        res = [self._rows[k] for k in r]
        return res

    def find_by_scan_template(self, tmp, rows):

        result = []
        for k, v in rows:
            if self.matches_template(v, tmp):
               result.append({k:v})

        return result

    def find_by_template(self, tmp, field_list=None, use_index=True):

        if tmp is None:
            new_t = CSVDataTable(table_name="Derived:" + self._table_name, loadit=True)
            new_t.load_from_rows(table_name="Derived:" + self._table_name, rows=self.get_rows())
            return new_t

        idx = self.get_best_index(tmp)
        logging.debug("Using index = %s", idx)

        if idx is None or use_index == False:
            result = self.find_by_scan_template(tmp, self.get_rows())
        else:
            idx = self._indexes[idx]
            res = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, res)

        if result:
            final_r = []
            for r in result:
                final_new_r = {k:r[k] for k in field_list}
                final_r.append(final_new_r)

        new_t = CSVDataTable(table_name="Derived:" + self._table_name, loadit=True)
        new_t.load_from_rows(table_name="Derived:" + self._table_name, rows=final_r)

        return new_t

    def find_by_template_rows(self, tmp, field_list=None, use_index=True):

        if tmp is None:
            return self._rows

        idx = self.get_best_index(tmp)
        logging.debug("Using index = %s", idx)

        if idx is None or use_index == False:
            result = self.find_by_scan_template(tmp, self.get_rows_with_rids())
        else:
            idx = self._indexes[idx]
            res = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, res)

        if result is not None:
            final_r = {}
            for r in result:
                if field_list is not None:
                    final_new_r = {k:r[k] for k in field_list}
                else:
                    final_new_r = r
                #final_r[]

            return final_r
        else:
            return None

    def insert(self, r):

        if self._rows is None:
            self._rows = {}
        rid = self._get_next_row_id()

        if self._indexes is not None:
            for k, v in self._indexes.items():
                v.add_to_index(r, rid)

        self._rows[rid] = copy.copy(r)

    def delete(self, tmp):

        result = self.find_by_template_rows(tmp)
        rows = result

        if rows is not None:
            for k in rows.keys():
                self._remove_rows(k)

    def import_data(self, rows):
        for r in rows:
            self.insert(r)

    def save(self):
        d = {
            "state": {
                "table_name": self._table_name,
                "primary_key_columns": self._primary_key_columns,
                "next_rid": self._get_next_row_id(),
                "column_names": self._column_names
            }
        }
        fn = CSVDataTable._default_directory + self._table_name + ".json"
        d["rows"] = self._rows

        for k, v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string
            d['indexes'] = idxs

        d = json.dumps(d, indent=2)
        with open(fn, "w+") as outfile:
            outfile.write(d)

    def load(self):

        fn = CSVDataTable._default_directory + self._table_name + ".json"
        with open(fn, "r") as infile:
            d = json.load(infile)

            state = d["state"]
            self._table_name = state["table_name"]
            self._primary_key_columns = state['primary_key_columns']
            self._next_row_id = state['next_rid']
            self._column_names = state["column_names"]
            self._rows = d['rows']

            for k, v in d["indexes"].items():
                idx = Index(loadit=v, table=self)
                if self._indexes is None:
                    self._indexes = {}
                self._indexes[k] = idx

    def load_from_rows(self, table_name, rows):
        result = table_name.load()



    @staticmethod
    def _get_scan_probe(l_table, r_table, on_clause):

        s_best, s_selective = l_table.get_index_and_selectivity(on_clause)
        r_best, r_selective = r_table.get_index_and_selectivity(on_clause)

        result = l_table, r_table

        if s_best is None and r_best is None:
            result = l_table, r_table
        elif s_best is None and r_best is not None:
            result = r_table, l_table
        elif s_best is not None and r_best is None:
            result = l_table, r_table
        elif s_best is not None and r_best is not None and s_selective < r_selective:
            result = r_table, l_table

        return result

    def _get_specific_where(self, wc):

        result = {}
        if wc is not None:
            for k,v in wc.items():
                kk = k.split(".")
                if len(kk) == 1:
                    result[k] = v
                elif kk[0] == self._table_name:
                    result[kk[1]] = v

        if result == {}:
            result = None

        return result

    def _get_specific_project(self, p_clause):

        result = []
        if p_clause is not None:
            for k in p_clause:
                kk = k.split(".")
                if len(kk) == 1:
                    result.append(k)
                elif kk[0] == self._table_name:
                    result.append(kk[1])

        if result == []:
            result = None

        return result

    @staticmethod
    def on_clause_to_where(on_c, r):

        result = {c:r[c] for c in on_c}
        return result

    def join(self, r_table, on_clause, w_clause, p_clause, optimize=True):

        if optimize:
            s_table, p_table = self._get_scan_probe(self, r_table, on_clause)
        else:
            s_table = self
            p_table = r_table

        if s_table != self and optimize:
            logging.debug("Swapping tables.")
        else:
            logging.debug("Not swapping tables.")

        logging.debug("Before pushdown, scan rows = %s ", len(s_table.get_rows()))

        if optimize:
            s_tmp = s_table._get_specific_where(w_clause)
            s_proj = s_table._get_specific_project(p_clause)

            s_rows = s_table.find_by_template(s_tmp, s_proj)
            logging.debug("After pushdown, scan rows = %s", len(s_rows.get_rows()))
        else:
            s_rows = s_table

        scan_rows = s_rows.get_rows()

        result = []

        for r in scan_rows:

            p_where = CSVDataTable.on_clause_to_where(on_clause, r)
            p_project = p_table._get_specific_project(p_clause)
            p_rows = p_table.find_by_template(p_where, p_project)
            p_rows = p_rows.get_rows()

            if p_rows:
                for r2 in p_rows:
                    new_r = {**r, **r2}
                    result.append(new_r)

        tn = "Join(" + self.get_table_name() + "," + r_table.get_table_name() + ")"
        final_result = CSVDataTable(
            table_name=tn,
            loadit=True)

        final_result.load_from_rows(table_name=tn, rows=result)

        return final_result
