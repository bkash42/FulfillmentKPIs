# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 14:20:39 2023
Edit on Wed Sep 13 2023 - @Frank


@author: bkashuba
"""

import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, BlobLeaseClient
from io import StringIO
import os
from dotenv import load_dotenv
import tempfile

### SETUP ###
# (Taxonomy1List) tax 1 list = https://4535487.app.netsuite.com/app/common/search/searchresults.nl?searchid=4125&saverun=T&whence=
# (Fulfillment KPI Data) fullDF = https://4535487.app.netsuite.com/app/common/search/searchresults.nl?searchid=4115&whence=

## Blob Storage Calls ##


def calculate_kpi():
    load_dotenv()

    #connecting to azure storage and container
    connectionString1 = os.environ["connectionString1"]
    blobConnect = BlobServiceClient.from_connection_string(connectionString1)
    containerName = "fulfillment-kpi"
    containerConnect = blobConnect.get_container_client(containerName)

    # aquiring blob files to load data frames
    tax1Blob = "Taxonomy1List.csv"
    dataBlob = "Fulfillment KPI Data.csv"
    tax1BlobClient = containerConnect.get_blob_client(tax1Blob)
    dataBlobClient = containerConnect.get_blob_client(dataBlob)
    tax1BlobData = tax1BlobClient.download_blob().content_as_text()
    dataBlobData = dataBlobClient.download_blob().content_as_text()

    #data frame creation
    fullDF = pd.read_csv(StringIO(dataBlobData), parse_dates=['Date'])
    Tax1List = pd.read_csv(StringIO(tax1BlobData))

    # Create a new row with the desired value in the 'Name' column
    new_row = pd.Series({'Name': 'Other/Charge'})

    # Append the new row to the Tax1List DataFrame
    Tax1List = Tax1List.append(new_row, ignore_index=True)

    # ###INPUT###
    # Get the current date
    today = datetime.now().date()

    #today = datetime.now().date()
    today = pd.to_datetime(today)

    # Calculate the date 7 days ago
    week_ago = today - timedelta(days=6)
    week_ago = pd.to_datetime(week_ago)

    # Create a new DataFrame named FS_full containing rows where the 'Subsidiary' column equals "Fix Supply"
    FS_full = fullDF[fullDF['Subsidiary'] == "Fix Supply"]

    # Create a new DataFrame named USA_full containing rows where the 'Subsidiary' column does not equal "Fix Supply"
    USA_full = fullDF[fullDF['Subsidiary'] != "Fix Supply"]


    ############USA########################


    ### Backlog ###


    # Create a new DataFrame named USA_BL containing rows from USA_full where the 'Backlog Status' column equals "Open"
    USA_BL = USA_full[USA_full['Backlog Status'] == "Open"]

    # Group USA_BL by 'Taxonomy 1' and sum up the 'Open Amount' column
    USA_BL_grouped = USA_BL.groupby('Taxonomy 1')['Open Amount'].sum()
    USA_BL_grouped = USA_BL_grouped.reset_index()


    ### Shipped Sales ###


    USA_SS = USA_full[USA_full['Type'] != "Sales Order"]
    USA_SS['Date'] = pd.to_datetime(USA_SS['Date'])


    ## Week


    # Create a boolean mask to filter rows where the 'Date' column is within the previous 7 days
    mask = (USA_SS['Date'] >= week_ago) & (USA_SS['Date'] <= today)

    # Create the new DataFrame by applying the mask to the original DataFrame
    USA_SS_W = USA_SS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    USA_SS_W = USA_SS_W.groupby('Taxonomy 1')['Amount'].sum()
    USA_SS_W = USA_SS_W.reset_index()

    ## Month


    # Create a boolean mask to filter rows where the 'Date' column is within the current month
    mask = (USA_SS['Date'].dt.year == today.year) & (USA_SS['Date'].dt.month == today.month)

    # Create the new DataFrame by applying the mask to the original DataFrame
    USA_SS_M = USA_SS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    USA_SS_M = USA_SS_M.groupby('Taxonomy 1')['Amount'].sum()
    USA_SS_M = USA_SS_M.reset_index()


    ### Processed Sales ###


    USA_PS = USA_full[USA_full['Type'] == "Sales Order"]
    USA_PS['Date'] = pd.to_datetime(USA_PS['Date'])


    ## Week


    # Create a boolean mask to filter rows where the 'Date' column is within the previous 7 days
    mask = (USA_PS['Date'] >= week_ago) & (USA_PS['Date'] <= today)

    # Create the new DataFrame by applying the mask to the original DataFrame
    USA_PS_W = USA_PS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    USA_PS_W = USA_PS_W.groupby('Taxonomy 1')['Amount'].sum()
    USA_PS_W = USA_PS_W.reset_index()

    ## Month


    # Create a boolean mask to filter rows where the 'Date' column is within the current month
    mask = (USA_PS['Date'].dt.year == today.year) & (USA_PS['Date'].dt.month == today.month)

    # Create the new DataFrame by applying the mask to the original DataFrame
    USA_PS_M = USA_PS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    USA_PS_M = USA_PS_M.groupby('Taxonomy 1')['Amount'].sum()
    USA_PS_M = USA_PS_M.reset_index()


    ######################FIX##################################

    ### Backlog ###

    # Create a new DataFrame named USA_BL containing rows from FS_full where the 'Backlog Status' column equals "Open"
    FS_BL = FS_full[FS_full['Backlog Status'] == "Open"]

    # Group FS_BL by 'Taxonomy 1' and sum up the 'Open Amount' column
    FS_BL_grouped = FS_BL.groupby('Taxonomy 1')['Open Amount'].sum()
    FS_BL_grouped = FS_BL_grouped.reset_index()

    ### Shipped Sales ### 

    FS_SS = FS_full[FS_full['Type'] != "Sales Order"]
    FS_SS['Date'] = pd.to_datetime(FS_SS['Date'])

    ## Week

    # Create a boolean mask to filter rows where the 'Date' column is within the previous 7 days
    mask = (FS_SS['Date'] >= week_ago) & (FS_SS['Date'] <= today)

    # Create the new DataFrame by applying the mask to the original DataFrame
    FS_SS_W = FS_SS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    FS_SS_W = FS_SS_W.groupby('Taxonomy 1')['Amount'].sum()
    FS_SS_W = FS_SS_W.reset_index()

    ## Month

    # Create a boolean mask to filter rows where the 'Date' column is within the current month
    mask = (FS_SS['Date'].dt.year == today.year) & (FS_SS['Date'].dt.month == today.month)

    # Create the new DataFrame by applying the mask to the original DataFrame
    FS_SS_M = FS_SS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    FS_SS_M = FS_SS_M.groupby('Taxonomy 1')['Amount'].sum()
    FS_SS_M = FS_SS_M.reset_index()

    ### Processed Sales ###

    FS_PS = FS_full[FS_full['Type'] == "Sales Order"]
    FS_PS['Date'] = pd.to_datetime(FS_PS['Date'])


    ## Week

    # Create a boolean mask to filter rows where the 'Date' column is within the previous 7 days
    mask = (FS_PS['Date'] >= week_ago) & (FS_PS['Date'] <= today)

    # Create the new DataFrame by applying the mask to the original DataFrame
    FS_PS_W = FS_PS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    FS_PS_W = FS_PS_W.groupby('Taxonomy 1')['Amount'].sum()
    FS_PS_W = FS_PS_W.reset_index()

    ## Month

    # Create a boolean mask to filter rows where the 'Date' column is within the current month
    mask = (FS_PS['Date'].dt.year == today.year) & (FS_PS['Date'].dt.month == today.month)

    # Create the new DataFrame by applying the mask to the original DataFrame
    FS_PS_M = FS_PS.loc[mask]

    # Group by 'Taxonomy 1' and sum up the 'Amount' column

    FS_PS_M = FS_PS_M.groupby('Taxonomy 1')['Amount'].sum()
    FS_PS_M = FS_PS_M.reset_index()

    ########## Bring in calculated values to summary DF ##########

    # USA Backlog Value

    Tax1List = pd.merge(Tax1List, USA_BL_grouped[['Taxonomy 1', 'Open Amount']], left_on='Name', right_on='Taxonomy 1', how='left')

    # Fill missing values in 'Open Amount' column
    Tax1List['Open Amount'] = Tax1List['Open Amount'].fillna(0)
    # Rename 'Open Amount' column to 'Backlog Amount USA'
    Tax1List = Tax1List.rename(columns={'Open Amount': 'USA | Open Sales Orders ($)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # USA Shipped Sales

    # Week

    # Bring Amount into Tax1List
    Tax1List = pd.merge(Tax1List, USA_SS_W[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')

    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'USA | Shipped Sales (Week)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)

    # Month 

    Tax1List = pd.merge(Tax1List, USA_SS_M[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')
    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'USA | Shipped Sales (MTD)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # USA Processed Sales

    # Week

    # Bring Amount into Tax1List
    Tax1List = pd.merge(Tax1List, USA_PS_W[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')

    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'USA | Processed Sales (Week)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # Month 


    Tax1List = pd.merge(Tax1List, USA_PS_M[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')
    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'USA | Processed Sales (MTD)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)



    #Fix Backlog Value


    Tax1List = pd.merge(Tax1List, FS_BL_grouped[['Taxonomy 1', 'Open Amount']], left_on='Name', right_on='Taxonomy 1', how='left')

    # Fill missing values in 'Open Amount' column

    Tax1List['Open Amount'] = Tax1List['Open Amount'].fillna(0)

    # Rename 'Open Amount' column to 'Backlog Amount Fix'

    Tax1List = Tax1List.rename(columns={'Open Amount': 'Fix Supply | Open Sales Orders ($)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # Fix Shipped Sales

    # Week

    # Bring Amount into Tax1List
    Tax1List = pd.merge(Tax1List, FS_SS_W[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')

    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'Fix Supply | Shipped Sales (Week)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # Month 


    Tax1List = pd.merge(Tax1List, FS_SS_M[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')
    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'Fix Supply | Shipped Sales (MTD)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # Fix Processed Sales


    # Week

    # Bring Amount into Tax1List
    Tax1List = pd.merge(Tax1List, FS_PS_W[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')

    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'Fix Supply | Processed Sales (Week)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # Month 


    Tax1List = pd.merge(Tax1List, FS_PS_M[['Taxonomy 1', 'Amount']], left_on='Name', right_on='Taxonomy 1', how='left')
    # Fill missing values in 'Amount' column
    Tax1List['Amount'] = Tax1List['Amount'].fillna(0)
    # Rename 'Amount' column
    Tax1List = Tax1List.rename(columns={'Amount': 'Fix Supply | Processed Sales (MTD)'})
    Tax1List = Tax1List.drop('Taxonomy 1', axis=1)


    # Output

    # Drop the 'Internal ID' column from the Tax1List DataFrame
    Tax1List = Tax1List.drop('Internal ID', axis=1)

    # Rename the 'Name' column to 'Taxonomy 1'
    Tax1List = Tax1List.rename(columns={'Name': 'Taxonomy 1'})

    # Save the Tax1List DataFrame to a csv file named "output.csv" without the index
    # Tax1List.to_csv("output.csv", index=False)

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        Tax1List.to_csv(temp_file.name,index=False)
        outputPath = temp_file.name

    uploadBlobClient = blobConnect.get_blob_client(container=containerName, blob="kpiOutput.csv")

    with open(file=outputPath,mode="rb") as outputData:
        uploadBlobClient.upload_blob(outputData, overwrite=True)

calculate_kpi()
