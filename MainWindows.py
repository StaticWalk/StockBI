from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QMainWindow
import matplotlib

from data.StockDataSyn import StockDataSyn

matplotlib.use('Qt5Agg')

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setGeometry(700, 300, 600, 300)
        MainWindow.setWindowIcon(QIcon('fun.png'))
        MainWindow.setWindowTitle("StockBI")
        MainWindow.setObjectName("MainWindow")
        self.centralWidget = QtWidgets.QWidget(MainWindow)
        self.centralWidget.setObjectName("centralWidget")
        self.pushButtonSelectStock = QtWidgets.QPushButton(self.centralWidget)
        self.pushButtonSelectStock.setGeometry(QtCore.QRect(310, 80, 221, 91))
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(20)
        self.pushButtonSelectStock.setFont(font)
        self.pushButtonSelectStock.setObjectName("pushButtonSelectStock")
        self.pushButtonStockData = QtWidgets.QPushButton(self.centralWidget)
        self.pushButtonStockData.setGeometry(QtCore.QRect(50, 80, 221, 91))
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
        self.pushButtonSelectStock.setText(_translate("MainWindow", "选股"))
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

    @pyqtSlot()
    def on_pushButtonSelectStock_clicked(self):
        print("选股")

    @pyqtSlot()
    def on_pushButtonStockData_clicked(self):
        # 更新日线数据已实现

        startData = "2020-04-07"
        endData = "2020-04-08"
        # endData = time.strftime("%Y-%m-%d", time.localtime()
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