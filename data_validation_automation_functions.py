import pandas as pd
import os

root = 'root directory'
target = 'target directory'

dirlist = [ item for item in os.listdir(root) if os.path.isdir(os.path.join(root, item)) ]
dates= pd.DataFrame(columns=['date'])
dates['date'] = dirlist
dates['date'] = pd.to_datetime(dates['date'])
dates.sort_values(by='date',ascending=True,inplace=True)
dates['date']=dates['date'].astype(str)
date_1 = dates['date'].iloc[-2]
date_2 = dates['date'].iloc[-1]

path = f'{target}/{date_1}-vs-{date_2}'
os.makedirs(path)

def process_sheet_1(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Snapshot_Date', 'Agency'])
    merged_df['difference'] = merged_df['Offender_Population_y'] - merged_df['Offender_Population_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Offender_Population_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int).abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_2(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Month','Year','Agency'])
    merged_df['difference'] = merged_df['Offender_Population_y'] - merged_df['Offender_Population_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Offender_Population_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int).abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_3(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Month','Year','Agency','Offender_Status'])
    merged_df['difference'] = merged_df['Offender_Population_y'] - merged_df['Offender_Population_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Offender_Population_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int).abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_4(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Month','Year','Department','Offender_Status'])
    merged_df['difference'] = merged_df['Offender_Population_y'] - merged_df['Offender_Population_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Offender_Population_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int).abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_5(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Year', 'Month','County','Offender_Status'])
    merged_df['difference'] = merged_df['Offender_Population_y'] - merged_df['Offender_Population_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Offender_Population_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int).abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_6(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['County','Year','Offender_Status'])
    merged_df['difference'] = merged_df['Offender_Population_y'] - merged_df['Offender_Population_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Offender_Population_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int).abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_7_by_age(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Snapshot_Year','Snapshot_Month','Agency','Inmate_Type','Age_Range','Sex'])
    merged_df['difference'] = merged_df['Inmates_y'] - merged_df['Inmates_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Inmates_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_7_by_race_ethnicity(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Snapshot_Year','Snapshot_Month','Agency','Inmate_Type','Race_Ethnicity','Sex'])
    merged_df['difference'] = merged_df['Inmates_y'] - merged_df['Inmates_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Inmates_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_7_by_sex(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Snapshot_Year','Snapshot_Month','Agency','Inmate_Type','Sex'])
    merged_df['difference'] = merged_df['Inmates_y'] - merged_df['Inmates_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Inmates_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_8_by_county_and_race_ethnicity(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Snapshot_Year','Snapshot_Month','Sex','Inmate_Type','County'])
    merged_df['difference'] = merged_df['Total_y'] - merged_df['Total_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Total_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_8_by_criminal_justice_agency_and_race_ethnicity(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Snapshot_Year','Snapshot_Month','Sex','Inmate_Type','Agency'])
    merged_df['difference'] = merged_df['Total_y'] - merged_df['Total_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Total_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_10_by_department(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Year','Department','Race_Ethnicity'])
    merged_df['difference'] = merged_df['Adjusted_Incarceration_Rate_y'] - merged_df['Adjusted_Incarceration_Rate_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Adjusted_Incarceration_Rate_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_10_by_sex(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Year','Race_Ethnicity','Sex'])
    merged_df['difference'] = merged_df['Adjusted_Incarceration_Rate_y'] - merged_df['Adjusted_Incarceration_Rate_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Adjusted_Incarceration_Rate_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_12_admissions_by_year(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'right', on = ['Agency','Admission_Type','Inmate_Type','Year'])
    merged_df['difference'] = merged_df['Count_per_Event_SubType_y'] - merged_df['Count_per_Event_SubType_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Count_per_Event_SubType_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_12_releases_by_year(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'right', on = ['Agency','Release_Type','Inmate_Type','Year'])
    merged_df['difference'] = merged_df['Count_per_Event_SubType_y'] - merged_df['Count_per_Event_SubType_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Count_per_Event_SubType_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_12_admissions_by_year_month(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'right', on = ['Agency','Admission_Type','Inmate_Type','Year_Month'])
    merged_df['difference'] = merged_df['Count_per_Event_SubType_y'] - merged_df['Count_per_Event_SubType_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Count_per_Event_SubType_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_12_releases_by_year_month(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'right', on = ['Agency','Release_Type','Inmate_Type','Year_Month'])
    merged_df['difference'] = merged_df['Count_per_Event_SubType_y'] - merged_df['Count_per_Event_SubType_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Count_per_Event_SubType_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_13_admissions(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Agency','Year','Month','Year_Month'])
    merged_df['difference'] = merged_df['Count_y'] - merged_df['Count_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Count_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def process_sheet_13_releases(sheet1,sheet2):
    merged_df = sheet1.merge(sheet2, how = 'inner', on = ['Agency','Year','Month','Year_Month'])
    merged_df['difference'] = merged_df['Count_y'] - merged_df['Count_x']
    merged_df['percentage'] = merged_df['difference']/merged_df['Count_y']*100
    merged_df['percentage'] = merged_df['percentage'].astype(int,errors='ignore').abs()
    merged_df.loc[merged_df['difference'] == 0, 'percentage'] = 0
    merged_df.loc[merged_df['percentage']>=5, 'Mismatch'] = 'Mismatch'
    merged_df.loc[merged_df['percentage']<5, 'Mismatch'] = 'Match'
    return merged_df

def run_program(date_1,date_2,root,target):
    #reading in previous weeks sheets
    P_sheet_1 = pd.read_excel(f'{root}/{date_1}/1_Offender_Population_by_Year.xlsx',sheet_name=1)
    P_sheet_2 =pd.read_excel(f'{root}/{date_1}/3_Offender_Population_by_Offender_Status.xlsx',sheet_name=1)
    P_sheet_3 =pd.read_excel(f'{root}/{date_1}/3_Offender_Population_by_Offender_Status.xlsx',sheet_name=1)
    P_sheet_4=pd.read_excel(f'{root}/{date_1}/4_Offender_Population_by_Offender_Status_over_Time.xlsx',sheet_name=1)
    P_sheet_5 = pd.read_excel(f'{root}/{date_1}/5_Offender_Population_by_County_by_Offender_Status.xlsx',sheet_name=1)
    P_sheet_6 =pd.read_excel(f'{root}/{date_1}/6_Offender_Population_by_Offender_Status_over_Time.xlsx',sheet_name=1)
    P_sheet_7_by_age = pd.read_excel(f'{root}/{date_1}/7_Offender_Population_by_Sex_and_Age_or_Race_Ethnicity.xlsx',sheet_name=1)
    P_sheet_7_by_race_ethnicity = pd.read_excel(f'{root}/{date_1}/7_Offender_Population_by_Sex_and_Age_or_Race_Ethnicity.xlsx',sheet_name=2)
    P_sheet_7_by_sex = pd.read_excel(f'{root}/{date_1}/7_Offender_Population_by_Sex_and_Age_or_Race_Ethnicity.xlsx',sheet_name=3)
    P_sheet_8_by_county_and_race_ethnicity = pd.read_excel(f'{root}/{date_1}/8_Offender_Population_by_Race_Ethnicity_and_County_or_Agency.xlsx',sheet_name=1)
    P_sheet_8_by_criminal_justice_agency_and_race_ethnicity = pd.read_excel(f'{root}/{date_1}/8_Offender_Population_by_Race_Ethnicity_and_County_or_Agency.xlsx',sheet_name=2)
    P_sheet_10_by_department = pd.read_excel(f'{root}/{date_1}/10_Incarceration_Rates_per_100000_by_Race_Ethnicity.xlsx',sheet_name=1)
    P_sheet_10_by_sex = pd.read_excel(f'{root}/{date_1}/10_Incarceration_Rates_per_100000_by_Race_Ethnicity.xlsx',sheet_name=2)
    P_sheet_12_by_year=pd.read_excel(f'{root}/{date_1}/12_Admission_and_Release_Counts.xlsx',sheet_name=1)
    P_sheet_12_by_year_month=pd.read_excel(f'{root}/{date_1}/12_Admission_and_Release_Counts.xlsx',sheet_name=2)
    P_sheet_13 = pd.read_excel(f'{root}/{date_1}/13_Admission_and_Release_Trends.xlsx',sheet_name=1)

    #reading in new weeks sheets
    N_sheet_1 = pd.read_excel(f'{root}/{date_2}/1_Offender_Population_by_Year.xlsx',sheet_name=1)
    N_sheet_2 =pd.read_excel(f'{root}/{date_2}/2_Offender_Population_over_Time.xlsx',sheet_name=1)
    N_sheet_3 =pd.read_excel(f'{root}/{date_2}/3_Offender_Population_by_Offender_Status.xlsx',sheet_name=1)
    N_sheet_4=pd.read_excel(f'{root}/{date_2}/4_Offender_Population_by_Offender_Status_over_Time.xlsx',sheet_name=1)
    N_sheet_5 = pd.read_excel(f'{root}/{date_2}/5_Offender_Population_by_County_by_Offender_Status.xlsx',sheet_name=1)
    N_sheet_6 =pd.read_excel(f'{root}/{date_2}/6_Offender_Population_by_Offender_Status_over_Time.xlsx',sheet_name=1)
    N_sheet_7_by_age = pd.read_excel(f'{root}/{date_2}/7_Offender_Population_by_Sex_and_Age_or_Race_Ethnicity.xlsx',sheet_name=1)
    N_sheet_7_by_race_ethnicity = pd.read_excel(f'{root}/{date_2}/7_Offender_Population_by_Sex_and_Age_or_Race_Ethnicity.xlsx',sheet_name=2)
    N_sheet_7_by_sex = pd.read_excel(f'{root}/{date_2}/7_Offender_Population_by_Sex_and_Age_or_Race_Ethnicity.xlsx',sheet_name=3)
    N_sheet_8_by_county_and_race_ethnicity = pd.read_excel(f'{root}/{date_2}/8_Offender_Population_by_Race_Ethnicity_and_County_or_Agency.xlsx',sheet_name=1)
    N_sheet_8_by_criminal_justice_agency_and_race_ethnicity = pd.read_excel(f'{root}/{date_2}/8_Offender_Population_by_Race_Ethnicity_and_County_or_Agency.xlsx',sheet_name=2)
    N_sheet_10_by_department = pd.read_excel(f'{root}/{date_2}/10_Incarceration_Rates_per_100000_by_Race_Ethnicity.xlsx',sheet_name=1)
    N_sheet_10_by_sex = pd.read_excel(f'{root}/{date_2}/10_Incarceration_Rates_per_100000_by_Race_Ethnicity.xlsx',sheet_name=2)
    N_sheet_12_by_year=pd.read_excel(f'{root}/{date_2}/12_Admission_and_Release_Counts.xlsx',sheet_name=1)
    N_sheet_12_by_year_month=pd.read_excel(f'{root}/{date_2}/12_Admission_and_Release_Counts.xlsx',sheet_name=2)
    N_sheet_13 = pd.read_excel(f'{root}/{date_2}/13_Admission_and_Release_Trends.xlsx',sheet_name=1)

    #parsing out admissions and releases for sheets 12/13
    P_sheet_13_grouped = P_sheet_13.groupby(['Event'])
    P_sheet_13_admissions = P_sheet_13_grouped.get_group('Admission')
    P_sheet_13_releases = P_sheet_13_grouped.get_group('Release')

    N_sheet_13_grouped = N_sheet_13.groupby(['Event'])
    N_sheet_13_admissions = N_sheet_13_grouped.get_group('Admission')
    N_sheet_13_releases = N_sheet_13_grouped.get_group('Release')

    P_sheet_12_by_year_grouped = P_sheet_12_by_year.groupby(['Event'])
    P_sheet_12_admissions_by_year = P_sheet_12_by_year_grouped.get_group('Admission')
    P_sheet_12_releases_by_year = P_sheet_12_by_year_grouped.get_group('Release')

    P_sheet_12_by_year_month_grouped = P_sheet_12_by_year_month.groupby(['Event'])
    P_sheet_12_admissions_by_year_month = P_sheet_12_by_year_month_grouped.get_group('Admission')
    P_sheet_12_releases_by_year_month = P_sheet_12_by_year_month_grouped.get_group('Release')

    N_sheet_12_by_year_grouped = N_sheet_12_by_year.groupby(['Event'])
    N_sheet_12_admissions_by_year = N_sheet_12_by_year_grouped.get_group('Admission')
    N_sheet_12_releases_by_year = N_sheet_12_by_year_grouped.get_group('Release')

    N_sheet_12_by_year_month_grouped = N_sheet_12_by_year_month.groupby(['Event'])
    N_sheet_12_admissions_by_year_month = N_sheet_12_by_year_month_grouped.get_group('Admission')
    N_sheet_12_releases_by_year_month = N_sheet_12_by_year_month_grouped.get_group('Release')

    #calling functions to process sheets
    sheet1 = process_sheet_1(P_sheet_1,N_sheet_1)

    sheet2 = process_sheet_2(P_sheet_2,N_sheet_2)

    sheet3 = process_sheet_3(P_sheet_3,N_sheet_3)

    sheet4 = process_sheet_4(P_sheet_4,N_sheet_4)

    sheet5 = process_sheet_5(P_sheet_5,N_sheet_5)

    sheet6 = process_sheet_6(P_sheet_6,N_sheet_6)

    sheet_7_by_age = process_sheet_7_by_age(P_sheet_7_by_age,N_sheet_7_by_age)
    sheet_7_by_race_ethnicity = process_sheet_7_by_race_ethnicity(P_sheet_7_by_race_ethnicity,N_sheet_7_by_race_ethnicity)
    sheet_7_by_sex = process_sheet_7_by_sex(P_sheet_7_by_sex,N_sheet_7_by_sex)

    sheet_8_by_county_and_race_ethnicity=process_sheet_8_by_county_and_race_ethnicity(P_sheet_8_by_county_and_race_ethnicity,N_sheet_8_by_county_and_race_ethnicity)
    sheet_8_by_criminal_justice_agency_and_race_ethnicity = process_sheet_8_by_criminal_justice_agency_and_race_ethnicity(P_sheet_8_by_criminal_justice_agency_and_race_ethnicity,N_sheet_8_by_criminal_justice_agency_and_race_ethnicity)

    sheet_10_by_department = process_sheet_10_by_department(P_sheet_10_by_department,N_sheet_10_by_department)
    sheet_10_by_sex = process_sheet_10_by_sex(P_sheet_10_by_sex,N_sheet_10_by_sex)

    sheet_12_admissions_by_year_month = process_sheet_12_admissions_by_year_month(P_sheet_12_admissions_by_year_month,N_sheet_12_admissions_by_year_month)
    sheet_12_admissions_by_year = process_sheet_12_admissions_by_year(P_sheet_12_admissions_by_year,N_sheet_12_admissions_by_year)
    sheet_12_releases_by_year_month = process_sheet_12_releases_by_year_month(P_sheet_12_releases_by_year_month,N_sheet_12_releases_by_year_month)
    sheet_12_releases_by_year = process_sheet_12_releases_by_year(P_sheet_12_releases_by_year,N_sheet_12_releases_by_year)

    sheet_13_admissions = process_sheet_13_admissions(P_sheet_13_admissions,N_sheet_13_admissions)
    sheet_13_releases = process_sheet_13_releases(P_sheet_13_releases,N_sheet_13_releases)

    #parsing joined dataframe back to separate weeks and writing to excel as separate sheets in workbooks
    sheet1_week1 = sheet1[['Snapshot_Date', 'Agency', 'Offender_Population_x', 'Percents_x']].copy()
    sheet1_week2 = sheet1[['Snapshot_Date', 'Agency', 'Offender_Population_y', 'Percents_y', 'difference', 'percentage','Mismatch']].copy()

    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/1_Offender_Population_by_Year.xlsx') as writer:  
        sheet1_week1.to_excel(writer, sheet_name='week_1')
        sheet1_week2.to_excel(writer, sheet_name='week_2_and_val')


    sheet2_week1 = sheet2[['Month', 'Year', 'Agency', 'Offender_Population_x']].copy()
    sheet2_week2 = sheet2[['Month', 'Year', 'Agency', 'Offender_Population_y','difference', 'percentage', 'Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/2_Offender_Population_over_Time.xlsx') as writer:  
        sheet2_week1.to_excel(writer, sheet_name='week_1')
        sheet2_week2.to_excel(writer, sheet_name='week_2_and_val')



    sheet3_week1 = sheet3[['Year', 'Month', 'Agency', 'Offender_Status', 'Offender_Population_x']].copy()
    sheet3_week2 = sheet3[['Year', 'Month', 'Agency', 'Offender_Status', 'Offender_Population_y','difference', 'percentage', 'Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/3_Offender_Population_by_Offender_Status.xlsx') as writer:  
        sheet3_week1.to_excel(writer, sheet_name='week_1')
        sheet3_week2.to_excel(writer, sheet_name='week_2_and_val')



    sheet4_week1 = sheet4[['Department', 'Year', 'Month', 'Offender_Status','Offender_Population_x']].copy()
    sheet4_week2 = sheet4[['Department', 'Year', 'Month', 'Offender_Status','Offender_Population_y','difference','percentage', 'Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/4_Offender_Population_by_Offender_Status_over_Time.xlsx') as writer:  
        sheet4_week1.to_excel(writer, sheet_name='week_1')
        sheet4_week2.to_excel(writer, sheet_name='week_2_and_val')



    sheet5_week1 = sheet5[['Year', 'Month', 'County', 'Offender_Status', 'Offender_Population_x']].copy()
    sheet5_week2 = sheet5[['Year', 'Month', 'County', 'Offender_Status', 'Offender_Population_y','difference','percentage','Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/5_Offender_Population_by_County_by_Offender_Status.xlsx') as writer:
        sheet5_week1.to_excel(writer, sheet_name='week_1')
        sheet5_week2.to_excel(writer, sheet_name='week_2_and_val')



    sheet6_week1 = sheet6[['County', 'Year', 'Offender_Status', 'Offender_Population_x']].copy()
    sheet6_week2 = sheet6[['County', 'Year', 'Offender_Status', 'Offender_Population_y','difference','percentage','Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/6_Offender_Population_by_Offender_Status_over_Time.xlsx') as writer:
        sheet6_week1.to_excel(writer, sheet_name='week_1')
        sheet6_week2.to_excel(writer, sheet_name='week_2_and_val')



    sheet_7_by_age_week1 = sheet_7_by_age[['Snapshot_Year', 'Snapshot_Month', 'Agency', 'Inmate_Type', 'Age_Range','Sex', 'Inmates_x', 'Percents_x']].copy()
    sheet_7_by_age_week2 = sheet_7_by_age[['Snapshot_Year', 'Snapshot_Month', 'Agency', 'Inmate_Type', 'Age_Range','Sex', 'Inmates_y', 'Percents_y','difference', 'percentage', 'Mismatch']].copy()
    sheet_7_by_race_ethnicity_week1 = sheet_7_by_race_ethnicity[['Snapshot_Year', 'Snapshot_Month', 'Agency', 'Inmate_Type','Race_Ethnicity', 'Sex', 'Inmates_x', 'Percents_x']].copy()
    sheet_7_by_race_ethnicity_week2 = sheet_7_by_race_ethnicity[['Snapshot_Year', 'Snapshot_Month', 'Agency', 'Inmate_Type','Race_Ethnicity', 'Sex', 'Inmates_y', 'Percents_y','difference', 'percentage','Mismatch']].copy()
    sheet_7_by_sex_week1 = sheet_7_by_sex[['Snapshot_Year', 'Snapshot_Month', 'Agency', 'Inmate_Type', 'Sex','Inmates_x', 'Percents_x']].copy()
    sheet_7_by_sex_week2 = sheet_7_by_sex[['Snapshot_Year', 'Snapshot_Month', 'Agency', 'Inmate_Type', 'Sex','Inmates_y', 'Percents_y','percentage','difference','Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/7_Offender_Population_by_Sex_and_Age_or_Race_Ethnicity.xlsx') as writer:
        sheet_7_by_age_week1.to_excel(writer, sheet_name='by_age_week1')
        sheet_7_by_age_week2.to_excel(writer, sheet_name='by_age_week2_and_val')
        sheet_7_by_race_ethnicity_week1.to_excel(writer, sheet_name='by_race_ethnicity_week1')
        sheet_7_by_race_ethnicity_week2.to_excel(writer, sheet_name='by_race_ethnicity_week2_and_val')
        sheet_7_by_sex_week1.to_excel(writer, sheet_name='by_sex__week1')
        sheet_7_by_sex_week2.to_excel(writer, sheet_name='by_sex__week2_and_val')
        

    sheet_8_by_county_and_race_ethnicity_week1 = sheet_8_by_county_and_race_ethnicity[['Snapshot_Year', 'Snapshot_Month', 'Sex', 'Inmate_Type', 'County','AmericanIndian_AlaskaNative_x', 'Asian_PacificIslander_x','Black_AfricanAmerican_x', 'Hispanic_Latino_x', 'Other_Unknown_x','White_x', 'Total_x']].copy()
    sheet_8_by_county_and_race_ethnicity_week2 = sheet_8_by_county_and_race_ethnicity[['Snapshot_Year', 'Snapshot_Month', 'Sex', 'Inmate_Type', 'County','AmericanIndian_AlaskaNative_y', 'Asian_PacificIslander_y','Black_AfricanAmerican_y', 'Hispanic_Latino_y', 'Other_Unknown_y','White_y', 'Total_y','difference','percentage','Mismatch']].copy()
    sheet_8_by_criminal_justice_agency_and_race_ethnicity_week1 = sheet_8_by_criminal_justice_agency_and_race_ethnicity[['Snapshot_Year', 'Snapshot_Month', 'Sex', 'Inmate_Type', 'Agency','AmericanIndian_AlaskaNative_x', 'Asian_PacificIslander_x','Black_AfricanAmerican_x', 'Hispanic_Latino_x', 'Other_Unknown_x','White_x', 'Total_x']].copy()
    sheet_8_by_criminal_justice_agency_and_race_ethnicity_week2 = sheet_8_by_criminal_justice_agency_and_race_ethnicity[['Snapshot_Year', 'Snapshot_Month', 'Sex', 'Inmate_Type', 'Agency','AmericanIndian_AlaskaNative_y', 'Asian_PacificIslander_y','Black_AfricanAmerican_y', 'Hispanic_Latino_y', 'Other_Unknown_y','White_y', 'Total_y','difference','percentage','Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/8_Offender_Population_by_Race_Ethnicity_and_County_or_Agency.xlsx') as writer:
        sheet_8_by_county_and_race_ethnicity_week1.to_excel(writer, sheet_name='by_county_and_RE_week1')
        sheet_8_by_county_and_race_ethnicity_week2.to_excel(writer, sheet_name='by_county_and_RE_week2_and_val')
        sheet_8_by_criminal_justice_agency_and_race_ethnicity_week1.to_excel(writer, sheet_name='by_CJ_agency_and_RE_week1')
        sheet_8_by_criminal_justice_agency_and_race_ethnicity_week2.to_excel(writer, sheet_name='by_CJ_and_RE_week2_and_val')

    sheet_10_by_department_week1 = sheet_10_by_department[['Year', 'Department', 'Race_Ethnicity', 'Adjusted_Incarceration_Rate_x']].copy()
    sheet_10_by_department_week2 = sheet_10_by_department[['Year', 'Department', 'Race_Ethnicity', 'Adjusted_Incarceration_Rate_y','difference','percentage','Mismatch']].copy()
    sheet_10_by_sex_week1 = sheet_10_by_sex[['Year', 'Race_Ethnicity', 'Sex', 'Adjusted_Incarceration_Rate_x']]
    sheet_10_by_sex_week2 = sheet_10_by_sex[['Year', 'Race_Ethnicity', 'Sex', 'Adjusted_Incarceration_Rate_y','difference','percentage','Mismatch']]
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/10_Incarceration_Rates_per_100000_by_Race_Ethnicity.xlsx.xlsx') as writer:
        sheet_10_by_department_week1.to_excel(writer, sheet_name='by_department_week1')
        sheet_10_by_department_week2.to_excel(writer, sheet_name='by_department_week2_and_val')
        sheet_10_by_sex_week1.to_excel(writer, sheet_name='by_sex_week1')
        sheet_10_by_sex_week2.to_excel(writer, sheet_name='by_sex_week2_and_val')


    sheet_12_admissions_by_year_week1 = sheet_12_admissions_by_year[['Agency', 'Admission_Type', 'Release_Type_x', 'Inmate_Type', 'Year','Event_x', 'Count_per_Event_SubType_x', 'Count_per_Event_Type_x']].copy()
    sheet_12_admissions_by_year_week2 = sheet_12_admissions_by_year[['Agency', 'Admission_Type', 'Release_Type_y', 'Inmate_Type', 'Year','Event_y', 'Count_per_Event_SubType_y', 'Count_per_Event_Type_y','difference','percentage','Mismatch']].copy()
    sheet_12_admissions_by_year_month_week1 = sheet_12_admissions_by_year_month[['Agency', 'Admission_Type', 'Release_Type_x', 'Inmate_Type','Year_Month', 'Event_x', 'Count_per_Event_SubType_x','Count_per_Event_Type_x']].copy()
    sheet_12_admissions_by_year_month_week2 = sheet_12_admissions_by_year_month[['Agency', 'Admission_Type', 'Release_Type_y', 'Inmate_Type','Year_Month', 'Event_y', 'Count_per_Event_SubType_y','Count_per_Event_Type_y','difference','percentage','Mismatch']].copy()
    sheet_12_releases_by_year_week1 = sheet_12_releases_by_year[['Agency', 'Admission_Type_x', 'Release_Type', 'Inmate_Type', 'Year','Event_x', 'Count_per_Event_SubType_x', 'Count_per_Event_Type_x']].copy()
    sheet_12_releases_by_year_week2 = sheet_12_releases_by_year[['Agency', 'Admission_Type_y', 'Release_Type', 'Inmate_Type', 'Year','Event_y', 'Count_per_Event_SubType_y', 'Count_per_Event_Type_y','difference','percentage','Mismatch']].copy()
    sheet_12_releases_by_year_month_week1 = sheet_12_releases_by_year_month[['Agency', 'Admission_Type_x', 'Release_Type', 'Inmate_Type', 'Year_Month','Event_x', 'Count_per_Event_SubType_x', 'Count_per_Event_Type_x']].copy()
    sheet_12_releases_by_year_month_week2 = sheet_12_releases_by_year_month[['Agency', 'Admission_Type_y', 'Release_Type', 'Inmate_Type', 'Year_Month','Event_y', 'Count_per_Event_SubType_y', 'Count_per_Event_Type_y','difference','percentage','Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/12_Admission_and_Release_Counts.xlsx') as writer:
        sheet_12_admissions_by_year_month_week1.to_excel(writer, sheet_name='admi_by_YM_week1')
        sheet_12_admissions_by_year_month_week2.to_excel(writer, sheet_name='admi_by_YM_week2_and_val')
        sheet_12_releases_by_year_month_week1.to_excel(writer, sheet_name='re_by_YM_week1')
        sheet_12_releases_by_year_month_week2.to_excel(writer, sheet_name='re_by_YM_week2_and_val')
        sheet_12_admissions_by_year_week1.to_excel(writer,sheet_name='admi_by_Y_week1')
        sheet_12_admissions_by_year_week2.to_excel(writer,sheet_name='admi_by_Y_week2_and_val')
        sheet_12_releases_by_year_week1.to_excel(writer,sheet_name='re_by_Y_week1')
        sheet_12_releases_by_year_week2.to_excel(writer,sheet_name='re_by_Y_week2_and_val')

    sheet_13_admissions_week1 = sheet_13_admissions[['Agency', 'Year', 'Month', 'Event_x', 'Year_Month', 'Count_x']].copy()
    sheet_13_admissions_week2 = sheet_13_admissions[['Agency', 'Year', 'Month', 'Event_y', 'Year_Month', 'Count_y','difference','percentage','Mismatch']].copy()
    sheet_13_releases_week1 = sheet_13_releases[['Agency', 'Year', 'Month', 'Event_x', 'Year_Month', 'Count_x']].copy()
    sheet_13_releases_week2 = sheet_13_releases[['Agency', 'Year', 'Month', 'Event_y', 'Year_Month', 'Count_y','difference','percentage','Mismatch']].copy()
    with pd.ExcelWriter(f'{target}/{date_1}-vs-{date_2}/13_Admission_and_Release_Trends.xlsx') as writer:
        sheet_13_admissions_week1.to_excel(writer, sheet_name='admissions_week1')
        sheet_13_admissions_week2.to_excel(writer, sheet_name='admissions_week2_and_val')
        sheet_13_releases_week1.to_excel(writer, sheet_name='re_week1')
        sheet_13_releases_week2.to_excel(writer, sheet_name='re_week2_and_val')

run_program(date_1,date_2)