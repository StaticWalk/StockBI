import time
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QMainWindow
import matplotlib

from data.StockDataSyn import StockDataSyn
from strategy.HighTurn import HighTurn
from strategy.KDJ1 import KDJ1

matplotlib.use('Qt5Agg')

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setGeometry(700, 300, 600, 300)
        MainWindow.setWindowIcon(QIcon('fun.png'))
        MainWindow.setWindowTitle("StockBI")
        MainWindow.setObjectName("MainWindow")
        self.centralWidget = QtWidgets.QWidget(MainWindow)
        self.centralWidget.setObjectName("centralWidget")

        self.pushButtonSelect1Stock = QtWidgets.QPushButton(self.centralWidget)
        self.pushButtonSelect1Stock.setGeometry(QtCore.QRect(310, 50, 221, 91))
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(20)
        self.pushButtonSelect1Stock.setFont(font)
        self.pushButtonSelect1Stock.setObjectName("pushButtonSelect1Stock")

        self.pushButtonSelectStock = QtWidgets.QPushButton(self.centralWidget)
        self.pushButtonSelectStock.setGeometry(QtCore.QRect(310, 150, 221, 91))
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(20)
        self.pushButtonSelectStock.setFont(font)
        self.pushButtonSelectStock.setObjectName("pushButtonSelectStock")

        self.pushButtonStockData = QtWidgets.QPushButton(self.centralWidget)
        self.pushButtonStockData.setGeometry(QtCore.QRect(50, 50, 221, 91))
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(20)
        self.pushButtonStockData.setFont(font)
        self.pushButtonStockData.setObjectName("pushButtonStockData")

        MainWindow.setCentralWidget(self.centralWidget)
        self.retranslateUi()
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self):
        _translate = QtCore.QCoreApplication.translate
        self.pushButtonSelectStock.setText(_translate("MainWindow", "kdj策略"))
        self.pushButtonSelect1Stock.setText(_translate("MainWindow", "高换手策略"))
        self.pushButtonStockData.setText(_translate("MainWindow", "更新股票数据"))

class MainWindows(QMainWindow,Ui_MainWindow):

    from pylab import mpl
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    mpl.rcParams['axes.unicode_minus'] = False

    def __init__(self):
        super(MainWindows,self).__init__()
        self.setupUi(self)
        self.show()
        self.stockDataSyn = StockDataSyn()
        self.kdj1 = KDJ1()
        self.highTurn = HighTurn()

    @pyqtSlot()
    def on_pushButtonSelect1Stock_clicked(self):
        print("高换手选股")
        # self.highTurn.run()

    @pyqtSlot()
    def on_pushButtonSelectStock_clicked(self):
        print("kdj选股")
        # self.kdj1.run()

    @pyqtSlot()
    def on_pushButtonStockData_clicked(self):
        # 日线数据更新已实现
        startData = "2020-04-05"
        endData = time.strftime("%Y-%m-%d", time.localtime())
        # self.stockDataSyn.updateMain(startData,endData)

if __name__ == '__main__':

    import ctypes
    import platform

    # 设置Windows底部任务栏图标
    if 'Windows' in platform.uname() :
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID('StockBI')

    import sys

    app = QApplication(sys.argv)
    MainWindows = MainWindows()

    sys.exit(app.exec_())