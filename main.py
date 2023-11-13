import pandas as pd
import random

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

processedKeys = []
master = []

transactionsKeys = []
transactMaster = []

path_transactions = "C:\\Users\\PatrickLin\\Documents\\trn_lines.csv"
path_creditNote = "C:\\Users\\PatrickLin\\Documents\\sc_lines.csv"

transactions = pd.read_csv(path_transactions)
creditNote = pd.read_csv(path_creditNote)

duplicateCount = 0

for index, row in creditNote.iterrows():
    currentKey = creditNote.loc[index, "CombinedKey"]
    currentZip = creditNote.loc[index, "ZIP_AREA__C"]
    # print(currentKey)

    zc = zipCount(currentZip, 1)
    ck = combinedKey(currentKey, [zc])

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

# Error Checking - Comparing total counts of CombinedKeys in Transaction Lines to Credit Note
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
                break

def cnCheck():
    testResult = "PASS"

    for i in master:
        x = i

        for y in x.zipList:
            testCount = 0

            for index, row in creditNote.iterrows():
                tempKey = creditNote.loc[index, "CombinedKey"]
                tempZip = creditNote.loc[index, "ZIP_AREA__C"]

                # print(tempKey)
                # print(tempZip)

                if tempKey == x.key and tempZip == y.zip:
                    testCount += 1
            
            if testCount != y.count:
                print("Invalid Key: " + x.key)
                print("Invalid Zip: " + y.zip)
                print("Master Zip Count: " + str(y.count))
                print("Test Count: " + str(testCount))
                testResult = "FAIL"
                return testResult

    return testResult
            
with open('output.txt', 'w') as f:
    f.write("---Credit Note Combination Key + Zip/Zip Counts---" + "\n\n")
    for i in master:
        f.write(i.key + "\n")

        for x in i.zipList:
            f.write("Zip: " + x.zip + "\n")
            f.write("Count: " + str(x.count) + "\n")
        
        f.write("\n")

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
                    mismatch += 1

                    if(total > y.count):
                        f.write("Valid: TRUE\n\n")
                    else:
                        f.write("Valid: FALSE\n\n")
                        invalid += 1
                else:
                    continue

    f.write("Total Unique Combination Keys - Credit Note: " + str(len(master)) + "\n")
    f.write("Total Duplicate Combination Keys in Credit Note: " + str(duplicateCount) + "\n")
    f.write("Total Combination Key Mismatches --- Credit Note VS Transaction Lines: " + str(mismatch) + "\n")
    f.write("Total Invalid Combination Key Count --- Credit Note < Transaction Lines: " + str(invalid) + "\n")
    # f.write("Validation of CN Combination Key + Zip Count Result: " + cnCheck())

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

    f.write("All Totals Match Sum of ZIP Counts: " + str(checkCount()) + "\n")

    def returnZIPCount(x):
        return x.count

    def sortZIP():
        for i in master:
            i.zipList.sort(key = lambda x: x.count)

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
    
    f.write("---Transaction Line Key Count---" + "\n\n")

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
    
    for index, row in transactions.iterrows():
        transactCK = transactions.loc[index, "CombinedKey"]

        try:
            matchedKey = next(x for x in master if x.key == transactCK)

            # f.write("Master Key: " + str(matchedKey.key) + "\n")
            # f.write("Transaction Key: " + str(transactCK) + "\n")
            # print(matchedKey.key)
            # print(transactCK)
        except:
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
        
    for i in transactMaster:
        f.write("Transaction Key: " + str(i.key) + "\n")
        f.write("Occurrence: " + str(i.count) + "\n\n")

    f.write("---Transaction VS Master ZIP Count---" + "\n\n")

    for i in transactMaster:
        try:
            matchedKey = next(x for x in master if x.key == i.key)
        except:
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
            f.write("Total Transact Count: " + str(i.count) + "\n\n")

            if count + i.count == matchedKey.total:
                continue
            # else:
            #     print("ERROR")
            #     print(matchedKey.key)
            #     print(matchedKey.total)
            #     print(matchedKey.totalRemoved)
            #     print(i.count)
        else:
            f.write("***Transaction Import Invalid - Please reference previous output***" + "\n\n")

    writer = pd.ExcelWriter('Processed Transactions.xlsx', engine = 'xlsxwriter')
    transactions.to_excel(writer, index = False, sheet_name = 'Data')
    writer.close()