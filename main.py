import pandas as pd
import numpy as np

class zipCount:
    def __init__(self, zip, count):
        self.zip = zip
        self.count = count

class combinedKey:
    def __init__(self, key, zipList):
        self.key = key
        self.zipList = zipList
        self.total = 0
        self.totalRemoved = 0

class transactKey:
    def __init__(self, key):
        self.key = key
        self.count = 1

class zip:
    def __init__(self, key, region):
        self.key = key
        self.region = region

class outlier:
    def __init__(self, key):
        self.key = key
        self.count = 1

processedKeys = []
master = []

transactionsKeys = []
transactMaster = []

zipMaster = []

# path_transactions = "C:\\Users\\patri\\Documents\\trn_lines.csv"
# path_creditNote = "C:\\Users\\patri\\Documents\\sc_lines.csv"

path_zipArea = "C:\\Users\\patri\\Documents\\ziparea.csv"
path_transactions = "C:\\Users\\patri\\Documents\\Geography V2\\TRNLines.csv"
path_creditNote = "C:\\Users\\patri\\Documents\\Geography V2\\SCNLines.csv"
path_WC = "C:\\Users\\patri\\Documents\\Geography V2\\WhiteCapFinal.csv"

transactions = pd.read_csv(path_transactions)
creditNote = pd.read_csv(path_creditNote)
zipArea = pd.read_csv(path_zipArea)
wcFinal = pd.read_csv(path_WC)

duplicateCount = 0
duplicateCountTransact = 0

# Fetch list of each ZIP and their associated region from ZIP Area Excel file
for index, row in zipArea.iterrows():
    currentZip = zipArea.loc[index, "ID"]
    currentRegion = zipArea.loc[index, "REGION__C"]

    newZip = zip(currentZip, currentRegion)

    zipMaster.append(newZip)

# Add in ZIP from WCFinal based on Branch #
def populateZip():
    missingBranches = 0
    presentBranches = 0

    with open('populateZIP.txt', 'w') as x:
        x.write("IMPORTING ZIP: WCFINAL --->>> SCN\n\n")
        x.write("---ROWS MISSING BRANCHES---\n")

        creditNote.insert(0, "ZIP", None, True)

        for index, row in creditNote.iterrows():

            if pd.isna(creditNote.loc[index, "BRANCH_NUMBER__C"]):
                x.write(str(row) + "\n\n")
                missingBranches += 1
                continue
            
            currentSCBranch = creditNote.loc[index, "BRANCH_NUMBER__C"].astype(int)

            # try:
            #     updateZip = wcFinal.query("BRANCH_NUMBER__C == 'currentSCBranch'")["ZIP_AREA__C"]
            #     print(updateZip[1])
            # except:
            #     print("ERROR")

            # creditNote.loc[index, "ZIP_AREA__C"] = updateZip
            newZip = pd.NA

            for wcIndex, wcRow in wcFinal.iterrows():
                if pd.isna(wcFinal.loc[wcIndex, "BRANCH_NUMBER__C"]):
                    continue
                
                currentWCBranch = wcFinal.loc[wcIndex, "BRANCH_NUMBER__C"].astype(int)

                if currentWCBranch == currentSCBranch:
                    newZip = wcFinal.loc[wcIndex, "ZIP_AREA__C"]
                    presentBranches += 1
                    break
            
            creditNote.at[index, "ZIP"] = newZip
            # presentBranches += 1
        
        x.write("\nTotal SCN Missing Branches: " + str(missingBranches) + "\n")
        x.write("Total SCN ZIPs Added: " + str(presentBranches))
        x.close()

populateZip()

# Correct ZIP based on whether the region matches in Credit Note
def checkZip():
    correctedZips = 0
    correctZips = 0
    NoZip = 0
    EmptyZip = 0
    MissingZip = 0

    with open('checkRegion.txt', 'w') as x:
        x.write("COMPARE REGION BASED ON MATCHING ZIP: SCN -> ZIP/AREA\n\n")
        x.write("---SCN ZIPS NOT FOUND IN ZIP/AREA TABLE---\n")

        counter = 0

        for index, row in creditNote.iterrows():
            currentZip = creditNote.loc[index, "ZIP"]
            currentRegion = creditNote.loc[index, "REGION__C"]

            # ---SKIPPED CONDITIONAL---
            # Skip if No Zip Available to avoid error. Counter to make sure skips match
            # if currentZip == "No Zip Available":
            #     NoZip += 1
            #     continue
            if counter == 8307:
                print(row)
                print(creditNote.loc[index, "ZIP"])
                print(type(creditNote.loc[index, "ZIP"]))
                print(creditNote.loc[index, "BRANCH_NUMBER__C"])
                print(type(creditNote.loc[index, "BRANCH_NUMBER__C"]))
                # print(np.isnan(creditNote.loc[index, "ZIP"]))
                print(pd.isna(creditNote.loc[index, "ZIP"]))
                print(pd.isnull(creditNote.loc[index, "ZIP"]))
                print(pd.isna(currentZip))
                # print(np.isnan(creditNote.loc[index, "BRANCH_NUMBER__C"]))
                print(pd.isna(creditNote.loc[index, "BRANCH_NUMBER__C"]))
                counter += 1
            else:
                counter += 1

            if pd.isna(creditNote.loc[index, "BRANCH_NUMBER__C"]) and pd.isnull(creditNote.loc[index, "ZIP"]):
                EmptyZip += 1
                continue
            elif not (pd.isna(creditNote.loc[index, "BRANCH_NUMBER__C"])) and pd.isna(creditNote.loc[index, "ZIP"]):
                MissingZip += 1
                x.write("***UNIQUE BRANCH***\n")
                x.write(str(row) + "\n\n")
                continue
            else:
                # Else, try finding the ZIP. If it doesn't exist in ZIP master list then print error
                try:
                    matchedKey = next(x for x in zipMaster if x.key == currentZip)
                except:
                    NoZip += 1
                    x.write(str(row) + "\n\n")
                    # print(currentZip)
                    # print(currentRegion)
                    # print("Error finding matching ZIP ---CONTINUING---")
                    continue
                    
                # Otherwise, we correct the region if it is different and then add a column as a flag 
                # indicating whether the region was changed for debugging. Then add one to counter 
                # for how many ZIPs were corrected
                if matchedKey.region != currentRegion:
                    creditNote.loc[index, "REGION__C"] = matchedKey.region
                    # creditNote.loc[index, "NEW REGION"] = "TRUE"
                    correctedZips += 1
                else:
                    correctZips += 1
                    # creditNote.loc[index, "NEW REGION"] = "FALSE"
                    continue
        
        x.write("Total Empty Branch + ZIP: " + str(EmptyZip) + "\n")
        x.write("Total Branches Not Found in WhiteCap List: " + str(MissingZip) + "\n")
        x.write("Total ZIPs Not Found in ZIP/AREA List: " + str(NoZip) + "\n")
        x.write("Total Corrected ZIPs: " + str(correctedZips) + "\n")
        x.write("Total Unmodified ZIPs: " + str(correctZips))
        x.close()
    
    # ---Debugging---
    # Print out total "No Zip Available" counted to make sure numbers match up
    # print("ZIP NOT FOUND: " + str(NoZip))

    # Return number of ZIPs where region was different
    # return correctedZips

checkZip()

# Altered ZIP correction of above function to work on Transactions
def checkZipFINAL():
    correctedZips = 0
    NoZip = 0

    for index, row in transactions.iterrows():
        currentZip = transactions.loc[index, "ZIP"]
        currentRegion = transactions.loc[index, "REGION__C"]

        if currentZip == "No Zip Available" or currentZip == "nan":
            NoZip += 1
            continue
        else:
            try:
                matchedKey = next(x for x in zipMaster if x.key == currentZip)
            except:
                print(transactions.loc[index, "CombinedKey"])
                print(currentZip)
                print(currentRegion)
                print("Error finding matching ZIP ---CONTINUING---")
                continue
        
            if matchedKey.region != currentRegion:
                transactions.loc[index, "REGION__C"] = matchedKey.region
                correctedZips += 1
            else:
                continue
    
    # Make a tuple out of the results and return it
    checkResults = (NoZip, correctedZips)
    return checkResults

# # Generate a Combo Key based on original formula for Credit Note
# # Do a comparison check first to make sure the combo keys don't match before replacing them.
# # Update NEW KEY flag for debugging
# def generateCK():
#     diffCount = 0

#     for index, row in creditNote.iterrows():
#         currentCK = creditNote.loc[index, "CombinedKey"]
#         currentNV = creditNote.loc[index, "C2G__NETVALUE__C"]

#         if currentNV.is_integer():
#             currentNV = creditNote.loc[index, "C2G__NETVALUE__C"].astype(int)
#         else:
#             currentNV = creditNote.loc[index, "C2G__NETVALUE__C"].astype(float)

#         newComboKey = str(creditNote.loc[index, "C2G__CREDITNOTE__C"]) + str(creditNote.loc[index, "C2G__DIMENSION1__C"]) \
#             + str(creditNote.loc[index, "SCMFFA__ITEM_NAME__C"]) + str(currentNV) \
#                 + str(creditNote.loc[index, "REGION__C"])
        
#         if currentCK != newComboKey:
#             creditNote.loc[index, "CombinedKey"] = newComboKey
#             creditNote.loc[index, "NEW KEY"] = "TRUE"
#             diffCount += 1

#             if creditNote.loc[index, "NEW REGION"] == "FALSE":
#                 print("ERROR KEY: " + newComboKey)
#                 print("Original Key: " + currentCK)
#         else:
#             creditNote.loc[index, "NEW KEY"] = "FALSE"
#             continue
    
#     return diffCount

# Generate a Combo Key based on original formula w/o Region for both Credit Note and Transactions
# Do this for every row since it will be a new key for everything
def generateCKV2():
    for index, row in creditNote.iterrows():
        currentNV = creditNote.loc[index, "C2G__NETVALUE__C"]

        if currentNV.is_integer():
            currentNV = creditNote.loc[index, "C2G__NETVALUE__C"].astype(int)
        else:
            currentNV = creditNote.loc[index, "C2G__NETVALUE__C"].astype(float)

        newComboKey = str(creditNote.loc[index, "C2G__CREDITNOTE__C"]) \
            + str(creditNote.loc[index, "C2G__DIMENSION1__C"]) + str(creditNote.loc[index, "SCMFFA__ITEM_NAME__C"]) + str(currentNV)
        
        creditNote.loc[index, "CombinedKey"] = newComboKey

    for index, row in transactions.iterrows():
        currentHV = transactions.loc[index, "C2G__HOMEVALUE__C"]

        if currentHV.is_integer():
            currentHV = transactions.loc[index, "C2G__HOMEVALUE__C"].astype(int)
        else:
            currentHV = transactions.loc[index, "C2G__HOMEVALUE__C"].astype(float)

        newTransactKey = str(transactions.loc[index, "C2G__TRANSACTION__R.C2G__SALESCREDITNOTE__C"]) \
            + str(transactions.loc[index, "C2G__DIMENSION1__C"]) + str(transactions.loc[index, "ITEM_MASTER__C"]) + str(currentHV)
        
        transactions.loc[index, "CombinedKey"] = newTransactKey


# print("Changed ZIP Count: " + str(checkZip()))
# print("Changed Combo Keys: " + str(generateCK()))

#----------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------
# IMPORT CODE
#----------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------

generateCKV2()

#Output the Transactions and Credit Note with new combo keys
writer = pd.ExcelWriter('Processed CN.xlsx', engine = 'xlsxwriter')
creditNote.to_excel(writer, index = False, sheet_name = 'Data')
writer.close()

writer = pd.ExcelWriter('Processed TR.xlsx', engine = 'xlsxwriter')
transactions.to_excel(writer, index = False, sheet_name = 'Data')
writer.close()

# # #---Debugging---
# # path_creditNoteV2 = "C:\\Users\\patri\\Documents\\Github\\Geography-Zip\\Processed CN.xlsx"
# # creditNoteV2 = pd.read_excel(path_creditNoteV2)

# # for index, row in creditNoteV2.iterrows():
# #     newRegion = creditNoteV2.loc[index, "NEW REGION"]
# #     newKey = creditNoteV2.loc[index, "NEW KEY"]
    
# #     if newRegion == "FALSE" and newKey == "TRUE":
# #         print(str(creditNoteV2.loc[index, "CombinedKey"]))
# #     else:
# #         continue

# Iterate through Credit Note and record the ZIPs associated with each Combo Key
# Keep track of how many new Combo Keys with master list
for index, row in creditNote.iterrows():
    currentKey = creditNote.loc[index, "CombinedKey"]
    currentZip = creditNote.loc[index, "ZIP"]
    # print(currentKey)

    zc = zipCount(currentZip, 1)
    ck = combinedKey(currentKey, [zc])

    #Conditional to check for missing ZIP
    if pd.isna(currentZip):
        continue

    if currentKey not in processedKeys:
        processedKeys.append(currentKey)
        ck.total += 1
        master.append(ck)
    else:
        for i in master:
            if i.key == currentKey:
                # print("Match Found!")
                # print(i.key)
                # print(currentKey)

                zipExists = False

                for x in i.zipList:
                    if x.zip == currentZip:
                        # print("Zip Match Found!")
                        # print(x.zip)
                        # print(currentZip)

                        x.count += 1
                        # print(str(x.count))

                        zipExists = True

                i.total += 1

                if zipExists == False:
                    i.zipList.append(zc)
                
                break
            else:
                continue
                    
        duplicateCount += 1

# Count unique combo keys and their repititions from transactions
for index, row in transactions.iterrows():
    currentKey = transactions.loc[index, "CombinedKey"]
    
    if currentKey not in transactionsKeys:
        transactionsKeys.append(currentKey)

        tk = transactKey(currentKey)
        transactMaster.append(tk)
    else:
        for i in transactMaster:
            if i.key == currentKey:
                i.count += 1
                duplicateCountTransact += 1
                break

# # Error Checking - Comparing total counts of CombinedKeys in Transaction Lines to Credit Note
# # Naive check to make sure the counts of ZIP and Combo Key all match up
# # Slow but accurate
# def cnCheck():
#     testResult = "PASS"

#     for i in master:
#         x = i

#         for y in x.zipList:
#             testCount = 0

#             for index, row in creditNote.iterrows():
#                 tempKey = creditNote.loc[index, "CombinedKey"]
#                 tempZip = creditNote.loc[index, "ZIP_AREA__C"]

#                 # print(tempKey)
#                 # print(tempZip)

#                 if tempKey == x.key and tempZip == y.zip:
#                     testCount += 1
            
#             if testCount != y.count:
#                 print("Invalid Key: " + x.key)
#                 print("Invalid Zip: " + y.zip)
#                 print("Master Zip Count: " + str(y.count))
#                 print("Test Count: " + str(testCount))
#                 testResult = "FAIL"
#                 return testResult

#     return testResult
            
with open('ZIP Count Output.txt', 'w') as f:
    countCR = 0
    countTR = 0

    f.write("---Credit Note Combination Key + Zip/Zip Counts---" + "\n\n")
    for i in master:
        f.write(i.key + "\n")

        for x in i.zipList:
            f.write("Zip: " + x.zip + "\n")
            f.write("Count: " + str(x.count) + "\n")
        
        f.write("\n")

    f.write("-----IMPORT VALIDATION-----\n\n")
    f.write("---Combination Key Checking---\n\n")
    mismatch = 0
    invalid = 0
    for i in master:
        total = 0

        for x in i.zipList:
            total += x.count

        for y in transactMaster:
            if i.key == y.key:
                if total != y.count:
                    f.write(i.key + "\n")
                    f.write("Credit Note count: " + str(total) + "\n")
                    f.write("Transaction Line count: " + str(y.count) + "\n")

                    countCR += total
                    countTR += y.count
                    mismatch += 1

                    if(total > y.count):
                        f.write("Valid: TRUE\n\n")
                    else:
                        f.write("Valid: FALSE\n\n")
                        invalid += 1
                else:
                    continue
    
    if mismatch == 0 and invalid == 0:
        f.write("Combo Key Check Results: Both files are 1-to-1\n")
    else:
        f.write("ERROR\n")
        f.write("Total Combination Key Mismatches --- Credit Note VS Transaction Lines: " + str(mismatch) + "\n")
        f.write("Total Invalid Combination Key Count --- Credit Note < Transaction Lines: " + str(invalid) + "\n")

    f.write("Total Unique Combination Keys - Credit Note: " + str(len(master)) + "\n")
    f.write("Total Unique Combination Keys - Transactions: " + str(len(transactMaster)) + "\n")
    f.write("Total Duplicate Combination Keys in Credit Note: " + str(duplicateCount) + "\n")
    f.write("Total Duplicate Combination Keys in Transactions: " + str(duplicateCountTransact) + "\n")
    f.write("Total Credit Note Mismatch Lines: " + str(countCR) + "\n")
    f.write("Total Transaction Mismatch Lines: " + str(countTR) + "\n\n")
    # f.write("Validation of CN Combination Key + Zip Count Result: " + cnCheck())

    #----------------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------------
    # EXPORT CODE
    #----------------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------------

    f.write("-----EXPORT VALIDATION-----\n\n")
    # -----Process Transactions file w/ Credit Note Results-----

    def checkCount():
        match = True

        for i in master:
            count = 0

            for x in i.zipList:
                count += x.count

            if count != i.total:
                match = False
                break
        
        return match

    # Sanity check that ZIP Counts still match
    f.write("All Totals Match Sum of ZIP Counts in CN: " + str(checkCount()) + "\n")

    def returnZIPCount(x):
        return x.count

    # Sort ZIP by ascending order
    def sortZIP():
        for i in master:
            i.zipList.sort(key = lambda x: x.count)

    # Check previous sort results
    def checkSort():
        sorted = True

        for i in master:
            count = 0

            for x in i.zipList:
                if count == 0:
                    count == x.count
                    continue
                else:
                    if count > x.count:
                        sorted = False
        
        return sorted
    
    sortZIP()

    f.write("Master List ZIPs Sorted Successfully: " + str(checkSort()) + "\n\n")

    # # Print Transaction Keys and their count
    # f.write("---Transaction Line Key Count---" + "\n\n")
    # for i in transactMaster:
    #     f.write("Transaction Key: " + str(i.key) + "\n")
    #     f.write("Occurrence: " + str(i.count) + "\n\n")

    def validImport(x):
        for y in transactMaster:
            if x.key == y.key:
                if x.total != y.count:
                    if(x.total > y.count):
                        return True
                    else:
                        return False
                else:
                    return True
    
    f.write("---Transaction Key Outliers---\n\n")

    # Count Outliers
    outlierTotal = 0 
    outlierMaster = []
    outliers = []
    validTransact = 0

    # For each transaction, try to find a matching combo key in master
    # If the combo key is a valid import, then begin assigning ZIPs to Transactions from Credit Note
    # Else, track the outlier key and output it later
    for index, row in transactions.iterrows():
        transactCK = transactions.loc[index, "CombinedKey"]

        try:
            matchedKey = next(x for x in master if x.key == transactCK)
            validTransact += 1
            # f.write("Master Key: " + str(matchedKey.key) + "\n")
            # f.write("Transaction Key: " + str(transactCK) + "\n")
            # print(matchedKey.key)
            # print(transactCK)
        except:
            if transactCK not in outlierMaster:
                outlierMaster.append(transactCK)
                newOutlier = outlier(transactCK)
                outliers.append(newOutlier)
                outlierTotal += 1
                continue
            else:
                for i in outliers:
                    if i.key == transactCK:
                        i.count += 1
                        outlierTotal += 1
                        break
                continue

        # matchedCount = next(y.count for y in transactMaster if y.key == transactCK)

        if validImport(matchedKey):
            for a in matchedKey.zipList:
                if(a.count == 0):
                    continue
                else:
                    transactions.loc[index, "ZIP"] = a.zip
                    a.count -= 1
                    matchedKey.totalRemoved += 1
                    break
        else:
            continue
    
    f.write("Total TR Processed: " + str(validTransact) + "\n")
    f.write("Total TR Outliers: " + str(outlierTotal) + "\n")
    f.write("Total Unique TR Outliers: " + str(len(outliers)) + "\n\n")
    for i in outliers:
        f.write("TR Key: " + i.key + "\n")
        f.write("Count: " + str(i.count) + "\n\n")

    # Count No Zip Available
    countNoZIP = 0
    for index, row in transactions.iterrows():
        if transactions.loc[index, "ZIP"] == "No Zip Available":
            countNoZIP += 1

    f.write("---Transaction VS Master ZIP Validation---" + "\n\n")

    invalidMissing = 0
    invalidMismatch = 0

    # Further validation of the results after assigning ZIPs
    for i in transactMaster:
        try:
            matchedKey = next(x for x in master if x.key == i.key)
        except:
            invalidMissing += 1
            f.write("Current Transaction Key: " + i.key + "\n")
            f.write("***IMPORT INVALID - Missing Key***\n\n")
            continue
        
        count = 0

        f.write("Current Transaction Key: " + i.key + "\n")
        f.write("Current Master Key: " + matchedKey.key + "\n")

        if validImport(matchedKey):
            for a in matchedKey.zipList:
                count += a.count

            f.write("Processed ZIP Count from Master: " + str(count) + "\n")
            f.write("Total Original ZIP from Master: " + str(matchedKey.total) + "\n")
            f.write("Total ZIP Removed from Master: " + str(matchedKey.totalRemoved) + "\n")
            f.write("Total Transact Count: " + str(i.count) + "\n")

            if i.count != matchedKey.totalRemoved:
                f.write("***FATAL DATA ERROR***\n\n")
            else:
                f.write("\n")

            # if count + i.count == matchedKey.total:
            #     continue
            # else:
            #     print("ERROR")
            #     print(matchedKey.key)
            #     print(matchedKey.total)
            #     print(matchedKey.totalRemoved)
            #     print(i.count)
        else:
            invalidMismatch += 1
            f.write("***IMPORT INVALID - Mismatch***\n\n")
        
    f.write("Total Invalid Missing: " + str(invalidMissing) + "\n")
    f.write("Total Invalid Mismatch: " + str(invalidMismatch) + "\n\n")

    f.write("---ZIP CORRECTION---\n\n")
    zipValidation = checkZipFINAL()
    f.write("Total No Zip Available: " + str(countNoZIP) + "\n")
    f.write("Total Zip Missing: " + str(zipValidation[0]) + "\n")
    f.write("Total Region Corrections: " + str(zipValidation[1]))

    writer = pd.ExcelWriter('Final Transactions.xlsx', engine = 'xlsxwriter')
    transactions.to_excel(writer, index = False, sheet_name = 'Data')
    writer.close()