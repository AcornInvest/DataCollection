import logging

class LogStringHandler(logging.Handler):
    def __init__(self, target_widget):
        #포매터 및 레벨, 출력할 대상 위젯 설정
        super(LogStringHandler, self).__init__()

        # 2 logger의 level을 INFO로 설정합니다.
        self.setLevel(logging.INFO)
        self.target_widget = target_widget

    def emit(self, record):
        #대상 위젯에 로그 내용을 추가한다
        msg = self.format(record)
        self.target_widget.append(msg)
        #self.target_widget.append(record.asctime + '--' + record.getMessage())
        #self.target_widget.append(record.getMessage())