import copy

from common.Common import *
from engine.StockDataEngine import StockDataEngine

class StockCodeDataTable(object):

    def __init__(self, mongoDbEngine , dataEngine,logThread):
        self.mongoDbEngine = mongoDbEngine
        self.dataEngine = dataEngine
        self._init()
        self.logThread = logThread

    def _init(self):

        self._stockCodesTable = {}
        self._fundCodesTable = {}
        self._sectorCodesTable = {}

        # only for updating CodeName table
        self.NewName_CodeNameDict = {}
        self.NewCode_CodeNameDict = {}
        self.Same_CodeNameDict = {}

    @property
    def stockFunds(self):
        return self._fundCodesTable

    @property
    def stockCodes(self):
        return self._stockCodesTable

    @property
    def stockAllCodes(self):
        """
            个股和大盘指数，不含基金（ETF）
        """
        return dict(self.stockCodes, **StockData.indexes)

    @property
    def stockAllCodesFunds(self):
        return dict(self.stockAllCodes, **self.stockFunds)

    def setStockCodes(self, code, name):
        if code in self._stockCodesTable:
            if name == self._stockCodesTable[code]:
                self.Same_CodeNameDict[code] = name
            else:
                self.NewName_CodeNameDict[code] = self._stockCodesTable[code] + '->' + name
                self._stockCodesTable[code] = name
        else:
            self.NewCode_CodeNameDict[code] = name
            self._stockCodesTable[code] = name


        """ @return: {new code:name}, {code:old name->new name}, {exit code:name} """
    def getAndSyncStockCodes(self):

        if len(self.Same_CodeNameDict) == len(self._stockCodesTable):
            return None, None, None

        if ( len(self.Same_CodeNameDict) + len(self.NewName_CodeNameDict) + len(self.NewCode_CodeNameDict) ) == len(self._stockCodesTable):
            return self.NewCode_CodeNameDict, self.NewName_CodeNameDict, None

        # 退市
        exit = {}
        for code in self._stockCodesTable:
            if code in self.Same_CodeNameDict: continue
            if code in self.NewName_CodeNameDict: continue
            if code in self.NewCode_CodeNameDict: continue

            exit[code] = self._stockCodesTable[code]

        assert(exit)

        # delete exit code from table
        for code in exit: del self._stockCodesTable[code]

        return self.NewCode_CodeNameDict, self.NewName_CodeNameDict, exit

    def set(self, codes):

        # set into object variables
        for code, name in codes.items():
            self.setStockCodes(code, name)

        # get changes and sync
        newCode, newName, exit = self.getAndSyncStockCodes()

        if newCode or newName:
            newNameTemp = {code: name[name.rfind('->') + 2:] for code, name in newName.items()}

            if not self.update2Db(dict(newCode, **newNameTemp)):
                return False

        # print updated result
        self.logThread.print(newCode, newName, exit)

        return True


    def update2Db(self, codes):

        # convert to MongoDB format
        codesForDb = [{'code':code, 'name':name} for code, name in codes.items()]

        # update into DB
        return self.mongoDbEngine.updateStockCodes(codesForDb)


    def update(self):
        self.logThread.print('开始更新股票代码表...')

        # first, load from DB
        self.load()

        codes = self.dataEngine.getStockCodesFromTuSharePro()
        if codes is None:
            self.logThread.print('更新股票代码表失败')
            return False

        if not self.set(codes):
            self.logThread.print('更新股票代码表失败')
            return False

        self.logThread.print('股票代码表更新完成')
        return True


    def load(self, codes=None):
        """
            indexes are always loaded by default
            @codes: None, load all stock codes, including indexes, funds, excluding sectors
                    [], not load any code(including funds), but only indexes
                    [code], load specified [code] with indexes
        """
        self.logThread.print('开始载入股票代码表...')

        # 初始化
        self._init()

        # copy so that not changing original @codes
        # 不改变传入参数的内容
        codes = copy.copy(codes)
        data = self.mongoDbEngine.getStockCodes(codes)

        if data is None:
            self.logThread.print('股票代码表载入失败')
            return False

        for doc in data:
            self._stockCodesTable[doc['code']] = doc['name']

        self.logThread.print('股票代码表载入完成')
        return True