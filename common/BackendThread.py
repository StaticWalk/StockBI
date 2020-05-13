import time
from PyQt5.QtCore import QThread , pyqtSignal, QDateTime


class BackendThread(QThread):
    # 通过类成员对象定义信号对象
    log = pyqtSignal(str)

    def print(self,x):
        self.log.emit("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),x))