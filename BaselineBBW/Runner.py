import os
import pandas as pd
import csv
import bbw
import json
from alive_progress import alive_bar

class bbwrunner():
    def __init__(self, model, table_path, outputPath):
        self.model = model
        self.table_path = table_path
        self.outputPath = outputPath
    OutputPath  = 'E://Submission/data/BaselineScoring/BBW_scoring/T2D/'
    datapath = 'E://data/TableSource/Limaye_exp/Ftables_instance/'

    def Runner(self):
        files = os.listdir(self.table_path)
        count = len(files)
        with alive_bar(count, bar='bubbles', spinner='notes2') as bar:
            for afile in files:
                afile = "file531575_0_cols1_rows31.csv"

                data = []
                tableFileName = self.table_path + afile
                output_file = self.outputPath + afile
                if os.path.exists(output_file):
                    pass

                if self.model == 'Limaye':

                    with open(tableFileName, encoding='latin1') as csvfile:
                        csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                        for oneGT in csvreader:
                            aline = []

                            for aCell in oneGT:
                                aCell = aCell.replace('"', '')
                                if len(aCell) == 0:
                                    continue
                                aline.append(aCell)
                            data.append(aline)

                if self.model == 'T2D':

                    with open(tableFileName, 'r', encoding='latin1') as load_f:
                        load_dict = json.load(load_f)
                        data = load_dict['relation']
                        data = list(map(list, zip(*data)))



                df = pd.DataFrame(data)
                print(df)

                cea_annotation = bbw.annotate(df)
                cea_candidate_list = cea_annotation.values.tolist()

                output_candidate_date = []

                for a_cell_annotation in cea_candidate_list:
                    cleaned_candidate_list = []
                    for a_candidate in a_cell_annotation[3]:
                        if 'http://www.wikidata.org/entity' in a_candidate[0]:
                            a_cleaned_candidate = a_candidate[0].replace("http://www.wikidata.org/entity/", "")
                            cleaned_candidate_list.append([a_cleaned_candidate, a_candidate[1]])
                    output_candidate_date.append([a_cell_annotation[1],a_cell_annotation[2],cleaned_candidate_list])

                with open(output_file, 'w', encoding ='latin1') as f:
                    writer = csv.writer(f)
                    writer.writerows(output_candidate_date)

                bar()
                break

if __name__ == "__main__":
    tablePath = 'E://data/TableSource/Limaye_exp/Ftables_instance/'
    outputPath = './test/'

    runner = bbwrunner('Limaye', tablePath, outputPath)
    runner.Runner()
