import urllib.parse

from PyQt5 import QtWidgets, QtCore
from pymongo import MongoClient
import time


class SeatMonitor(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        username = urllib.parse.quote_plus("shuifanzz")
        password = urllib.parse.quote_plus("shuifanzz")
        client = MongoClient(
            "mongodb+srv://" + username + ":" + password + "@cluster0.hksonbt.mongodb.net/?retryWrites=true&w=majority")
        self.db = client['p1']
        self.seats = self.db['seat_map']
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Seat Monitor')
        self.setGeometry(100, 100, 500, 500)

        self.seat_boxes = []
        for i in range(20):
            seat_box = QtWidgets.QWidget(self)
            seat_box.setGeometry(50 + (i % 5) * 80, 50 + (i // 5) * 80, 60, 60)
            seat_box.setStyleSheet('background-color: green')
            self.seat_boxes.append(seat_box)

        self.update_seats()
        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.update_seats)
        self.timer.start(1000)

        self.show()

    def update_seats(self):
        seat = self.seats.find()[0]
        for i, seat_box in enumerate(self.seat_boxes):
            ##seat = self.seats.find_one({'seat_number': i})
            if seat['occupied_' + str(i)]:
                seat_box.setStyleSheet('background-color: gray')
            else:
                seat_box.setStyleSheet('background-color: green')


if __name__ == '__main__':
    app = QtWidgets.QApplication([])
    win = SeatMonitor()
    app.exec_()