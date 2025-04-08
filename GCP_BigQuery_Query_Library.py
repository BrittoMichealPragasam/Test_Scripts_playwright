# google_cloud_utils.py
import logging
from io import BytesIO, TextIOWrapper
import datetime
import os
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import json

class GoogleCloudBigQueryHelper:

    def __init__(self):
        self.client = bigquery.Client()
        self.storage_client = storage.Client()
    def bq_get_table_data(self, query: str):
        dfTablequeryResults = pd.DataFrame()
        try:
            dfTablequeryResults = self.client.query_and_wait(f"{query}").to_dataframe()
            return dfTablequeryResults
        except Exception as e:
            print(f"Error in bq_get_table_data: {e}")

    def bq_read_file_data_buckets(self, bucket_name: str, folder_path: str, file_name: str):
        dfFileData = pd.DataFrame()
        if folder_path.strip() == "":
            strFilePath = file_name.strip()
        else:
            strFilePath = f"{folder_path.strip()}/{file_name.strip()}"
        try:
            # Construct the Google Cloud Storage blob URL
            blob_url = f"gs://{bucket_name.strip()}/{strFilePath}"
            # Read the CSV file directly from the blob URL in chunks
            dfFileData = pd.concat([chunk for chunk in pd.read_csv(blob_url, chunksize=1000000)], ignore_index=True)
            # Display the resulting DataFrame with split columns
            return dfFileData
        except Exception as e:
            print(f"Error in bq_read_file_data_buckets: {e}")

    def fetch_schema_names_json(self,bucket_name, folder_path, file_name):
        if folder_path.strip() == "":
            strFilePath = file_name
        else:
            strFilePath = f"{folder_path}/{file_name}"
        try:
            objbucket = self.storage_client.bucket(bucket_name)
            blob = objbucket.blob(strFilePath)
            json_data = json.loads(blob.download_as_string())
            lisNamesAttributes = [item['name'] for item in json_data]
            return lisNamesAttributes
        except Exception as e:
            print(f"Error in fetch_schema_names_json: {e}")
    def bq_replace_fileheader_with_schema(self, dfFileData, lisSchemaNames):
        try:
            lisSplitColumns = dfFileData.columns[0].split('~')
            delimiter = '~'
            # Determine the number of columns to keep
            intActualNumOfColumnsForDF = abs(len(lisSplitColumns) - len(lisSchemaNames))
            # Combine new column names using the delimiter, up to intActualNumOfColumnsForDF
            strNewColumnName = delimiter.join(lisSchemaNames[intActualNumOfColumnsForDF:])
            dfFileData.columns = [strNewColumnName]
            # Split the header
            header = dfFileData.columns[0].split('~')
            # Split the data rows
            df_split = pd.concat([dfFileData.iloc[:, 0].str.split('~', expand=True)], ignore_index=True)
            df_split = df_split.applymap(lambda x: x.replace('"', '') if isinstance(x, str) else x)
            # Assign the split header to the DataFrame
            df_split.columns = header
            return df_split
        except Exception as e:
            print(f"Error in bq_replace_fileheader_with_schema: {e}")

class DataFramesComparision:
    def Compare_DataFrames(self, df1, df2, pk_keys: list):
        Time_stamp = (datetime.datetime.now())
        Time_stamp = str(Time_stamp)
        Time_stamp = Time_stamp.replace('-', '')
        Time_stamp = Time_stamp.replace(':', '')
        Time_stamp = Time_stamp.replace('.', '')
        Time_stamp = Time_stamp.replace(' ', '')
        Duplicate_Source = ['Duplicate_Souce_Flag', 'False']
        Duplicate_Target = ['Duplicate_Target_Flag', 'False']

        # Functions
        def comp(list1, list2):
            fc = 0
            for i in range(len(list1)):
                if list1[i] != list2[i]:
                    fc += 1
                    break
                else:
                    pass
            if fc >= 1:
                return False
            else:
                return True

        # Importing Source and Target data
        # inputs = pd.read_excel('input.xlsx',dtype=str)
        inputs1 = pk_keys
        # Source_Sep = inputs['Source_Delimeter']
        # Target_Sep = inputs['Target_Delimeter']
        # ColumnToReplace_Source = inputs['ColumnToReplace_Source']
        # ColumnToReplace_Source = ColumnToReplace_Source.dropna()
        # ColumnToReplace_Target = inputs['ColumnToReplace_Target']
        # ColumnToReplace_Target = ColumnToReplace_Target.dropna()
        # S_RplaceTo = inputs['S_ReplaceTo']
        # S_ReplaceBy = inputs['S_ReplaceBy']
        # T_RplaceTo = inputs['T_ReplaceTo']
        # T_ReplaceBy = inputs['T_ReplaceBy']
        # S_ReplaceBy.fillna('',inplace=True)
        # T_ReplaceBy.fillna('',inplace=True)
        # S_TextQualifier = inputs['S_TextQualifier']
        # T_TextQualifier = inputs['T_TextQualifier']
        # S_TextQualifier.fillna('',inplace=True)
        # T_TextQualifier.fillna('',inplace=True)
        # S_Column_to_ignore = inputs['S_Column_to_ignore']
        # T_Column_to_ignore = inputs['T_Column_to_ignore']

        BW_PSA = df1
        Hue_Data = df2
        Hue_Data = Hue_Data.astype(str)
        BW_PSA = BW_PSA.astype(str)
        # if S_TextQualifier[0] !='':
        #         BW_PSA = pd.read_csv('everseen_assisted_scanning_accuracy_source.csv',dtype=str,quotechar = S_TextQualifier[0],delimiter=r'\s*,\s*',engine ='python')
        # else:
        #         BW_PSA = pd.read_csv('everseen_assisted_scanning_accuracy_source.csv',dtype=str,delimiter=Source_Sep[0],engine ='python')

        # if T_TextQualifier[0] != '':
        #     Hue_Data = pd.read_csv('everseen_assisted_scanning_accuracy_target.csv',dtype=str,quotechar = T_TextQualifier[0],delimiter=r'\s*,\s*',engine ='python')
        # else:
        #     Hue_Data = pd.read_csv('everseen_assisted_scanning_accuracy_target.csv',dtype=str,delimiter=Target_Sep[0],engine ='python')

        Hue_Data.insert(0, 'ROW_NUMBER', range(1, 1 + len(Hue_Data)))
        BW_PSA.insert(0, 'ROW_NUMBER', range(1, 1 + len(BW_PSA)))
        inputs = pk_keys
        # inputs = inputs.dropna()

        # Replacement
        # n=0
        # for p in ColumnToReplace_Source:
        #     BW_PSA[p] = BW_PSA[p].str.replace(S_RplaceTo[n],S_ReplaceBy[n])
        #     n += 1
        # n=0
        # for p in ColumnToReplace_Target:
        #     Hue_Data[p] = Hue_Data[p].str.replace(T_RplaceTo[n],T_ReplaceBy[n])
        #     n += 1

        # Delete not required columns
        # BW_PSA = BW_PSA.drop(S_Column_to_ignore, axis=1,errors ='ignore')
        # BW_PSA = BW_PSA.drop(T_Column_to_ignore, axis=1,errors ='ignore')
        # Hue_Data = Hue_Data.drop(T_Column_to_ignore, axis=1,errors ='ignore')
        ##--------------------------------------------------------------
        # Variables
        pass_count = 0
        failed_count = 0
        total_count = BW_PSA.shape[0] * BW_PSA.shape[1]
        result = []
        result.append(BW_PSA.columns)
        Source_Missing = []

        # Replacing Na
        BW_PSA.fillna(' ', inplace=True)
        Hue_Data.fillna(' ', inplace=True)

        
        # Main Loop for Validation
        for i in range(BW_PSA.shape[0]):

            key1 = []
            for inp in inputs:
                key1.append(BW_PSA[inp][i])

            flag = -1

            record_set = Hue_Data[(Hue_Data[inputs[0]] == key1[0])]
            for j in range(len(inputs)):
                record_set = record_set[(record_set[inputs[j]] == key1[j])]
            resRow = []
            if len(record_set) != 0:

                failed_flag = 0
                for k in BW_PSA.columns:
                    if k != 'ROW_NUMBER':
                        Comparison1 = str(BW_PSA[k][i])
                        Comparison12 = str(list(record_set[k])[0])
                        if Comparison1 == Comparison12:
                            resRow.append("Pass")
                            pass_count += 1
                        else:
                            resRow.append("Failed (" + str(Comparison1) + ',' + str(Comparison12) + ')')
                            failed_count += 1
                            failed_flag += 1
                    else:
                        resRow.append(str(BW_PSA[k][i]))
                if failed_flag != 0:
                    result.append(resRow)
                flag = -1
            else:
                flag = i
            if len(record_set) > 1:
                Duplicate_Target = ['Duplicate_Target_Flag', 'True']
            if (flag == i):
                msg = []
                msg_str = ""
                Source_Missing.append(BW_PSA.iloc[i])
        Source_Missing_df = pd.DataFrame(Source_Missing)
        excell_df = pd.DataFrame(result)

        # Extra in Target
        left_records = []

        for j in range(Hue_Data.shape[0]):
            key3 = []

            for inp3 in inputs:
                key3.append((Hue_Data[inp3][j]))

            flag = -1
            record_set = BW_PSA[(BW_PSA[inputs[0]] == key3[0])]
            for n in range(len(inputs)):
                record_set = record_set[(record_set[inputs[n]] == key3[n])]

            if len(record_set) != 0:

                flag = -1

            else:
                flag = j
            if len(record_set) > 1:
                Duplicate_Source = ['Duplicate_Source_Flag', 'True']
            if (flag == j):
                left_records.append(Hue_Data.iloc[j])

        left_records_df = pd.DataFrame(left_records)

        # Excel Work Export

        writer = pd.ExcelWriter('Comparison_Output_' + Time_stamp + '.xlsx', engine='xlsxwriter')
        workbook = writer.book
        worksheet_1 = workbook.add_worksheet('Script_Info')

        Source_Missing_df.to_excel(writer, sheet_name='Extra_In_Source', header=True, index=False)
        excell_df.to_excel('result.xlsx', header=False, index=False)
        excell_df = pd.read_excel('result.xlsx')

        for col in excell_df.columns:
            failed_count_1 = 0
            Pass_count = 0
            for row in excell_df[col]:
                if row != "Pass":
                    failed_count_1 += 1
                    break
            if failed_count_1 == 0:
                del excell_df[col]

        # Extra column in Target
        Extra_T = []
        Extra_T.append('Extra_Columns_Target')
        for col in Hue_Data.columns:
            if col not in BW_PSA:
                Extra_T.append(col)

        excell_df.to_excel(writer, sheet_name='Output', header=True, index=False)

        Hue_Data.to_excel(writer, sheet_name='Target', header=True, index=False)
        BW_PSA.to_excel(writer, sheet_name='Source', header=True, index=False)
        left_records_df.to_excel(writer, sheet_name='Extra_In_Target', header=True, index=False)
        # inputs1.to_excel(writer, sheet_name='Input',header=True, index=False)
        worksheet = writer.sheets['Output']

        format1 = workbook.add_format({'bg_color': '#FFC7CE',
                                       'font_color': '#9C0006'})

        worksheet.conditional_format('A2:XFD1048576', {'type': 'cell',
                                                       'criteria': 'not equal to',
                                                       'value': '"Pass"',
                                                       'format': format1})

        merge_format = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'left',
            'valign': 'vcenter',
            'fg_color': 'yellow',
            'text_wrap': True
        })

        worksheet_1.merge_range('A1:I9', """Script Name		:  Comparison Script Version 1.1
        Script Description	: To Compare two xlsx file with multiple primary keys
        Input Parameters	: Input file with Primary key column list
        Output Parameters      :  Nil
        Author		        :  Sai Jagannath Gondesi
        Creation Date  	:  08-Jan-2020""", merge_format)

        worksheet_1.write_column('J1', Extra_T)
        worksheet_1.write_column('K1', Duplicate_Source)
        worksheet_1.write_column('L1', Duplicate_Target)
        chart = workbook.add_chart({'type': 'pie'})

        data = [
            ['PASS %', 'FAIL %', 'EXTRA %'],
            [pass_count * 100 / (BW_PSA.shape[0] * BW_PSA.shape[1] - BW_PSA.shape[0]),
             failed_count * 100 / (BW_PSA.shape[0] * BW_PSA.shape[1] - BW_PSA.shape[0]),
             (BW_PSA.shape[0] * BW_PSA.shape[1] - BW_PSA.shape[0] - pass_count - failed_count) * 100 / (
                         BW_PSA.shape[0] * BW_PSA.shape[1] - BW_PSA.shape[0])],
        ]

        worksheet_1.write_column('M1', data[0])
        worksheet_1.write_column('N1', data[1])

        chart.add_series({
            'categories': '=Script_Info!$M$1:$M$3',
            'values': '=Script_Info!$N$1:$N$3',
            'points': [
                {'fill': {'color': 'green'}},
                {'fill': {'color': 'red'}},
                {'fill': {'color': 'blue'}},
            ],
        })

        worksheet_1.insert_chart('M4', chart)
        workbook.close()