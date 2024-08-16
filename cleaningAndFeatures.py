"""
I like to put my work in functions and keep my functions in scripts. This allows me
to easily trace issues with my code to isolated chunks, and keep the actual notebook cleaner.
Rarely does anyone need to see the nitty gritty of my functions, but if the need arises I can
send a file with my functions. It also improves the reproducibility of my code for others.

Often, I find that my functions that write pickles are extremely general and do not merit
being written twice. This allows me to work smarter, not harder.
"""
import pandas as pd

def replaceNAs(df):
    ## consider data without NAs
    noNA = df[df['Age'].notnull()]

    ## Group by and take the mean of all combinations of factors
    means = noNA.groupby(['Job Type', 'Tax Classification'], as_index = False)[['Age','Income']].mean()

    output = df.merge(means, how = 'left', on = ['Job Type', 'Tax Classification'], suffixes = ('', '_temp'))
    output['Age'] = output['Age'].fillna(output['Age_temp'])
    output['Income'] = output['Income'].fillna(output['Income_temp'])
    output.drop(['Age_temp', 'Income_temp'], axis = 1, inplace = True)
    
    return output


def cleanAdv(df):
    
    '''
    This function will clean the advisory dataset by 1) replacing misspellings in Job Type
    and Tax Classifcation and 2) imputing the mean value for Income and Age for the missing 
    observations.
    '''
    ## clean the job type
    df.loc[df['Job Type'] == 'Vr', 'Job Type'] = 'VP'
    
    ## clean the tax classification
    df.loc[df['Tax Classification'] == 'Lod', 'Tax Classification'] = 'Low'
    df.loc[df['Tax Classification'] == 'Mediud', 'Tax Classification'] = 'Medium'
    df.loc[df['Tax Classification'] == 'Higd', 'Tax Classification'] = 'High'
    
    df.loc[df['Income'].notnull(), 'Income'] = round(df['Income'].loc[df['Income'].notnull()],0)
    df.loc[df['Age'].notnull(), 'Age'] = round(df['Age'].loc[df['Age'].notnull()],0)
    
    ## set job type and classification as categories and reorder them
    df['Tax Classification'] = df['Tax Classification'].astype('category')
    df['Job Type'] = df['Job Type'].astype('category')
    
    df['Tax Classification'].cat.reorder_categories(['Low', 'Medium','High'])
    
    cleaned = replaceNAs(df)
    
    return cleaned




def createNumTransactions(df):
    dfGrouped = df.groupby(['ID'], as_index = False)['Transaction_Type'].size()
    dfGrouped = df.groupby(['ID'], as_index=False)['Transaction_Type'].size()
    dfGrouped.rename(columns={'size':'numTransactions'}, inplace=True)

    
    return dfGrouped

def mostRecentTrans(df):
    '''
    I'm choosing to interpret the most recent transaction as the amount and type, but no date.
    Nothing in the challenge suggests that the recency matters, therefore I will not consider it.
    This is 100% a question I would ask a client to validate my understanding is consistent with theirs.
    '''
    ## Identify the most recent transaction by date
    dfGrouped = df.groupby(['ID'], as_index = False)['Transaction_Date'].max()
    
    ## Inner join on ID and date such that only the most recent observations remain
    df = df.merge(dfGrouped, how = 'inner',
                  on = ['ID', 'Transaction_Date'])[['ID', 'Amount', 'Transaction_Type']]
    
    ## rename columns for clarity
    df.rename(columns = {'Amount': 'mostRecentAmount',
                         'Transaction_Type': 'mostRecentType'},
             inplace = True)
    
    return df

def mostCommonTrans(df):
    '''
    This will return the most common type of transaction if that is unique. If there are
    multiple most common types, then 'Multiple' is returned.
    '''
    ## Count the number of transactions by type
    countType = df.groupby(['ID', 'Transaction_Type'], as_index = False)['Transaction_Type'].size()

    ## Determine the maximum frequency per ID
    maxID = countType.groupby(['ID'], as_index = False)['size'].max()

    ## join in the max to a combination of the two datasets
    compare = countType.merge(maxID, how = 'left', on = 'ID', suffixes = ('','Max'))

    ## filter to the rows where the size = maxSize
    maxOnly = compare[compare['size'] == compare['sizeMax']].reset_index(drop = True)

    ## Identify if values are unique
    identifyUnique = maxOnly.groupby(['ID'], as_index = False)['Transaction_Type'].size()

    ## merge in the dataframe with all the logic applied to determine unique most common
    allLogic = maxOnly.merge(identifyUnique, how = 'left', on = 'ID', suffixes = ('', 'Unique'))


    uniOnly = allLogic[['ID','Transaction_Type']][allLogic['sizeUnique'] == 1].reset_index(drop = True)

    allIDs = maxID[['ID']].merge(uniOnly, how = 'left', on = 'ID')
    allIDs.fillna('Multiple', inplace = True)
    allIDs.rename(columns = {'Transaction_Type': 'mostFrequentTrans'}, inplace = True)
    
    return allIDs

def totalAmount(df):
    dfGrouped = df.groupby(['ID'], as_index = False)['Amount'].sum()
    dfGrouped.rename(columns={'Amount' : 'totalAmount'}, inplace = True)
    
    return dfGrouped


def cleanPrepFeatures(df):
    ## Initialize a DF with all IDs
    output = pd.DataFrame({'ID': range(1,201)})
    
    
    ## first clean the dates
    df['Transaction_Date'] = pd.to_datetime(df['Transaction_Date'])
    
    ## create the number of transactions
    numTrans = createNumTransactions(df)
    
    ## Identify the most recent transaction
    mostRecent = mostRecentTrans(df)
    
    ## Identify the most common transaction
    mostCommon = mostCommonTrans(df)
    
    ## Sum the dollar amounts
    totalA = totalAmount(df)
    
    ## Join these in order to add the columns
    output = output.merge(numTrans, how = 'left', on = 'ID')
    output = output.merge(mostRecent, how = 'left', on = 'ID')
    output = output.merge(mostCommon, how = 'left', on = 'ID')
    output = output.merge(totalA, how = 'left', on = 'ID')
    
    ## Fill the NAs logically
    output['numTransactions'] = output['numTransactions'].fillna(0)
    output['mostRecentAmount'] = output['mostRecentAmount'].fillna(0)
    output['mostRecentType'] = output['mostRecentType'].fillna('None')
    output['mostFrequentTrans'] = output['mostFrequentTrans'].fillna('None')
    output['totalAmount'] = output['totalAmount'].fillna(0)
    
    return output