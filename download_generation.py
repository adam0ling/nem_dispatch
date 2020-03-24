import pandas as pd
import numpy as np
import urllib.request
import datetime as dt
import calendar
from zipfile import ZipFile
import os
import shutil


# funcs
def ceil_dt(dt_x, delta):
    '''
    Rounds datetime to nearest halfhour. \n
    from: https://stackoverflow.com/questions/32723150/rounding-up-to-nearest-30-minutes-in-python \n

    dt_x - datetime in question
    delta - time interval in question (30mins)
    '''
    return dt_x + (dt.datetime.min - dt_x) % delta

def makeURLs(start=[0, 0, 0], end=[0, 0, 0]):
    '''
    Overcomplicated function that generates a list of URLs needed for downloads for interval in the same month. \n 
    start - start date as list [YEAR, MONTH, DAY], default - first day of last month \n 
    end - end date as list [YEAR, MONTH, DAY], , default - last day of last month \n
    '''
    # set defaults
    today = dt.date.today()
    year = today.year
    month = today.month -1
    base_url = 'http://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_'

    if start == [0, 0, 0]:
        start = [year, month, 1]
    if end == [0, 0, 0]:
        end = [year, month, calendar.monthrange(year, month)[1]]

    # start
    print(
        'Generating URLs from {} to {}'.format(
            dt.date(start[0], start[1], start[2]),
            dt.date(end[0], end[1], end[2]))
            )

    urls = []
    year = str(start[0])
    month = str(start[1])
    if len(month)==1: month = '0' + month
    for i in range(start[2], end[2]+1, 1):
        day = str(i)
        if len(day)==1: 
            day = '0' + day
        new_url = base_url + year + month + day + '.zip'
        urls.append(new_url)
        # print(new_url)
    print('Done.')

    return urls


def downloadGen(download_to_directory, start=[0, 0, 0], end=[0, 0, 0]):
    '''
    Overcomplicated function that downloads and extracts generation for interval in the same month. 
    FYI it extracts zipped files in the selected directory and overwrites if any exist 
    with the same name. Suggested to create a fresh directory. \n 
    download_to_directory - path to download directory \n
    start - start date as list [YEAR, MONTH, DAY], default - first day of last month \n 
    end - end date as list [YEAR, MONTH, DAY], , default - last day of last month \n
    '''
    # make a directory
    new_dir = os.path.abspath(download_to_directory)
    new_dir = os.path.join(new_dir, 'temp')
    try:
        os.mkdir(new_dir)
        print("Directory " , new_dir ,  " Created ") 
    except FileExistsError:
        print("Directory " , new_dir ,  " already exists")
    
    # get urls
    urls = makeURLs(start, end)

    # start download    
    print('Downloading all zips to {}'.format(new_dir))

    all_zips = []
    for url in urls:
        print('Downloading {}'.format(url))
        toExport = os.path.join(new_dir,str(url)[-12:])
        urllib.request.urlretrieve(url, toExport)
        all_zips.append(toExport)

    print('Download finished.')

    # extracting
    print('Start extracting')
    print('Extracting day files')
    all_mini_zips = []
    day_dirs = []
    for i in all_zips:
        new_dir_i = i[:-4]
        day_dirs.append(new_dir_i) 
        os.mkdir(i[:-4])
        with ZipFile(i,'r') as zipObj:
            listOfFileNames = zipObj.namelist()
            all_mini_zips.append(listOfFileNames)
            zipObj.extractall(path=new_dir_i)

    print('Removing day zips')
    for i in all_zips:
        os.remove(i)

    print('Extracting 5min zips')
    all_5min = []
    counter = 0
    for i in day_dirs:
        for j in all_mini_zips[counter]:
            filePath = os.path.join(i,j)
            with ZipFile(filePath,'r') as zipObj:
                listOfFileNames = zipObj.namelist()
                all_5min.append(listOfFileNames)
                zipObj.extractall(path=new_dir)
        counter += 1

    print('Done extracting files.')
    print('Removing unneeded zips')
    for i in day_dirs:
        shutil.rmtree(i)

    print('Generating all file names')
    fileNames = []
    for i in all_5min:
        newPath = os.path.join(new_dir, i[0])
        fileNames.append(newPath)

    print('Done')
    return fileNames


def generationFile(download_to_directory, farm_code, start=[0, 0, 0], end=[0, 0, 0]):
    '''
    Overcomplicated function that downloads, extracts and concats generation files for interval in the same month. 
    FYI it extracts zipped files in the selected directory and overwrites if any exist 
    with the same name. Suggested to create a fresh directory. \n 
    download_to_directory - path to download directory \n
    farm_code - farm code such as BANN1 \n
    start - start date as list [YEAR, MONTH, DAY], default - first day of last month \n 
    end - end date as list [YEAR, MONTH, DAY], , default - last day of last month \n
    '''
    filePaths = downloadGen(download_to_directory, start, end)
    
    print('Reading and concatening files')
    df = pd.DataFrame()
    for i in filePaths:
        # print('Reading file: {}'.format(i))
        new_df = pd.read_csv(i,header=1)[:-1]
        df = pd.concat([df, new_df], axis=0)

    # important columns
    important_cols = df.columns[4:]
    df = df[important_cols]
    df = df[df['DUID']==farm_code]
    
    save_path = os.path.join(download_to_directory, 'generationFile-{}.csv'.format(farm_code))
    df.to_csv(save_path, index=False)
    print(
        'Generation file: {}, has been saved in {}'.format(
            'generationFile-{}.csv'.format(farm_code), 
            os.path.join(download_to_directory, 'generationFile-{}.csv'.format(farm_code))))

    return save_path

def generationFile30(download_to_directory, farm_code, start=[0, 0, 0], end=[0, 0, 0]):
    '''
    Overcomplicated function that downloads, extracts and concats generation files for interval in the same month. 
    FYI it extracts zipped files in the selected directory and overwrites if any exist 
    with the same name. Suggested to create a fresh directory. \n 
    download_to_directory - path to download directory \n
    farm_code - farm code such as BANN1 \n
    start - start date as list [YEAR, MONTH, DAY], default - first day of last month \n 
    end - end date as list [YEAR, MONTH, DAY], , default - last day of last month \n
    '''
    # read file
    filePath = generationFile(download_to_directory, farm_code, start, end)
    initDf = pd.read_csv(filePath)
    initDf.drop_duplicates()  # from my testing duplicated values are the same
    
    print('Turning 5min intervals to 30min intervals.') 
    print('Would be great to use this function in life, am I right???')
    # date play
    dates = pd.to_datetime(initDf[initDf.columns[0]])
    initDf['DATE'] = dates
    
    initDf['DATE_new'] = initDf['DATE'].dt.ceil('30min')
    newDf = initDf[['DATE_new', 'SCADAVALUE']].groupby(by=['DATE_new']).mean()
    newPath = os.path.join(download_to_directory, 'generationFile30-{}.csv'.format(farm_code))
    newDf.to_csv(newPath)

    print('30min generation file saved to {}'.format(newPath))

    return newDf