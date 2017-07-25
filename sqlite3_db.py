# -*- coding: utf-8 -*-
# sqlite3_db.py
#
"""SQLite3 データベースクラス.

既存の SQLite3 モジュールのカスタマイズクラス.
"""

import sqlite3
from logging import getLogger, StreamHandler, Formatter, WARNING
from collections import OrderedDict


class SQLite3DB:
    """SQLite3 データベースへの接続・各種操作を行うクラス."""

    loglevel = WARNING

    def __init__(self, db_uri: str):
        """コンストラクタ.

        各種初期化を行う。
        :param db_uri: データベースファイルのURI
        """
        self._db_uri = db_uri
        self._conn = None
        self._cur = None
        self._isConnect = False

        # ロガーのセットアップ.
        self._setup_logger()
        # DBへの接続.
        self.connect(self._db_uri)

    def __enter__(self):
        """コンテキストマネージャの実装 入口."""
        self.debug("enter into 'with' statement.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャの実装. 出口.

        with文経由でインスタンスが作成され、
        with文から抜けるときDBのコミット、クローズを行う.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.close()
        self.debug("exit frow 'with' statement.")

    def _setup_logger(self):
        """ロガーのセットアップを行う."""
        # ロガー作成.
        self._logger = getLogger(str(self))
        self._logger.setLevel(self.loglevel)
        # コンソールハンドラの作成.
        ch = StreamHandler()
        ch.setLevel(self.loglevel)
        # フォーマッターの作成.
        formatter = Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        # ロガーにコンソールハンドラを追加.
        self._logger.addHandler(ch)
        # ログを他の名前空間に伝播させないようにしておく.
        self._logger.propagate = False

    def info(self, msg: str):
        """infoレベルのログメッセージを送信する.

        :param msg: ログとして送信するメッセージ
        """
        self._logger.info(msg)

    def debug(self, msg: str):
        """debugレベルのログメッセージを送信する.

        :param msg: ログとして送信するメッセージ
        """
        self._logger.debug(msg)

    def warning(self, msg: str):
        """warningレベルのログメッセージを送信する.

        :param msg: ログとして送信するメッセージ
        """
        self._logger.warning(msg)

    #
    # DB接続系
    #
    def connect(self, db_uri=None) -> sqlite3.Connection:
        """引数で指定したDBへの接続を行い、sqlite3.Connectionオブジェクトを返す.

        :param db_uri: データベースファイルのURI
        """
        # DBファイルのURIが渡されなかったら以前の接続DBに接続する.
        if db_uri is None:
            self._conn = sqlite3.connect(self._db_uri)
        else:
            self._db_uri = db_uri
            self.debug("DB接続開始. db_uri={}".format(db_uri))
            self._conn = sqlite3.connect(self._db_uri)
            self._cur = self._conn.cursor()
            self._isConnect = True

            return self._conn

    def close(self):
        """接続しているデータベースのコミット, クローズを行う."""
        if self._isConnect:
            self._conn.commit()
            self._conn.close()
            self._isConnect = False
            self._conn = None
            self._cur = None

            self.debug("DB接続終了. db_uri={}".format(self._db_uri))

    def is_connected(self) -> bool:
        """DBへの接続状況を確認 ON-True OFF-False.

        :return: True or False
        """
        return self._isConnect

    def get_db_uri(self) -> str:
        """データベースURIを返す.

        :return: データベースURI.
        """
        return self._db_uri

    def get_connection(self) -> sqlite3.Connection:
        """現在の接続 sqlite3.Connectionオブジェクトを返す.

        :return: sqlite3.Connection
        """
        return self._conn

    def get_cursor(self) -> sqlite3.Cursor:
        """カーソルを返す. sqlite3.Cursorオブジェクトを返す.

        :return: sqlite3.
        """
        return self._cur

    def get_column_label(self, table_name: str) -> tuple:
        """指定したテーブルのカラムラベルの一覧をタプルにして返す.

        :table_name: テーブル名
        :return: カラムラベルのタプル
        """
        self._conn = self.get_connection()
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.cursor()
        cur.execute('select * from ?', ('{}'.format(table_name),))
        row = cur.fetchone()
        self.debug("row のオブジェクト型は {} です.".format(type(row)))
        return tuple(row.keys())

    #
    # SQL文実行系メソッド
    #
    def execute(self, sql: str, params=None) -> sqlite3.Cursor:
        """第2引数で渡されたSQL文を実行. sqlite3.Cursorオブジェクトを返す.

        :param sql: 実行するSQL文.
        :param params: 割り当て用のパラメータ　要タプル型
        :return: sqlite3.Cursor
        """
        if not self.is_connected():
            self.connect()

        cur = self.get_cursor()
        self.debug("""Execute following SQL statement.
             SQL:{0} params:{1}""".format(sql, params))
        if params is None:
            cur.execute(sql)
        else:
            # 割り当てパラメータがタプルでなかった場合例外を発生させる.
            if not isinstance(params, tuple):
                params = tuple(params)
                if not isinstance(params, tuple):
                    raise ValueError("割り当てパラメータはタプルで渡してください.")
            cur.execute(sql, params)
        # 念のためcommitしておく. 必要なければこの行は削除する.
        self._conn.commit()
        self.debug("SQL文の実行が完了.")
        return cur

    def executemany(self, sqls, params):
        """複数のSQL文を実行する　第2引数で渡されたリストの要素分実行する.

        :param sqls: 実行するSQL文
        :param params: 割り当て用パラメータ 要タプル型
        """
        # 割り当てパラメータがタプルでなかった場合は例外を発生させる.
        if not isinstance(params, tuple):
            params = tuple(params)
            if not isinstance(params, tuple):
                raise ValueError("割り当てパラメータはタプルで渡してください.")

        if not self.is_connected():
            self.connect()

        cur = self.get_cursor()
        self.debug("""Execute following SQL statement.
            SQL: {0} params{1}""".format(sqls, params))
        cur.executemany(sqls)
        self.get_connection().commit()

    def fetchone(self):
        """クエリ結果から次のrowをフェッチして、1つのシーケンスを返す."""
        return self._cur.fetchone()

    def fetchall(self):
        """全てのクエリ結果のrowをフェッチして、リストを返す."""
        return self._cur.fetchall()

    #
    # テーブル操作.
    #
    def exists(self, table_name: str) -> bool:
        """テーブルの存在確認 return True or False.

        :param table_name: テーブル名
        :return: True | False
        """
        self.debug("テーブル {} が存在するかどうかを確認します.".format(table_name))
        sql = (
            "select count(*) "
            "from sqlite_master "
            "where type='table' "
            "and name=?"
        )
        cur = self.execute(sql, (table_name,))
        count = cur.fetchone()

        if count is not None:
            return True
        else:
            return False

    def create_table(self, table_name: str, structure: list):
        """テーブルを作成する.

        :param table_name: テーブル名
        :param structure: テーブル構造(
            カラムラベルと、SQLデータタイプを示す文字列を組としたタプルのリスト)

        実行SQL例:
        CREATE TABLE <テーブル名>　<(カラム名1 データ型, カラム名2 データ型, ...)>
        """
        self.debug("テーブル {} を作成します.".format(table_name))

        # カラムの整形
        def _label_sanitizer(label: str) -> str:
            return "'{}'".format(label)

        # テーブル構造.
        strc = '('
        first = True
        for t in structure:
            if not first:
                strc += ","
            else:
                first = False
            column_label, data_type = t
            strc += '{} {}'.format(_label_sanitizer(column_label), data_type)
        strc += ')'

        # SQL文生成.
        sql = "CREATE TABLE {} {}".format(table_name, strc)
        # 実行.
        self.execute(sql)

    def create_table_as_text_type(self, table_name: str, column_label: tuple):
        """データ型 'text' でテーブルを作成する.

        :param table_name: テーブル名
        :param column_label: カラム名のタプル
        """
        # テーブル構造の作成.
        data_types = tuple(['text' for i in range(len(column_label))])
        table_structure = list(zip(column_label, data_types))
        # テーブルの作成.
        self.create_table(table_name=table_name, structure=table_structure)

    def drop_table(self, table_name: str):
        """指定のテーブルを削除する.

        :param table_name: テーブル名
        実行SQL例:
        DROP TABLE <>テーブル名.
        """
        self.debug("テーブル {} を削除します.".format(table_name))
        # SQL文生成.
        sql = "DROP TABLE {}".format(table_name)
        self.execute(sql=sql)

    def count(self, table_name: str):
        """指定したテーブルのレコード数をカウントする.

        :param table_name: テーブル名
        """
        query = self.select_all(table_name=table_name, select_columns=('count(*)',)).fetchone()
        if isinstance(query, int):
            return query
        elif isinstance(query, tuple):
            for i in query:
                return i
        else:
            raise LookupError("レコードが存在しません.")

    def select_all(self, table_name: str, select_columns=None) -> sqlite3.Cursor:
        """指定したテーブルからすべてのレコードをSELECTする.

        :param table_name: テーブル名
        :param select_columns: SELECT対象となるカラム名
        :return:
        """
        if select_columns is None:
            select_columns = ('*',)
        return self.select(table_name=table_name,
                           select_columns=select_columns,
                           conditions=OrderedDict())

    def select(self,
               table_name: str,
               select_columns: tuple,
               conditions: OrderedDict) -> sqlite3.Cursor:
        """指定した条件で SELECT を行う.

        :param table_name: テーブル名
        :param select_columns: select対象となるカラム名
        :param conditions: 条件節 キーワードをキーとし、値としてその条件を持つOrderedDict
        ex.{'where': '"a"="b"', 'or': '"c"="d"'}
        :return: sqlite3.Cursor
        """
        if not isinstance(conditions, OrderedDict):
            raise TypeError("conditionsはOrderedDict型で渡さなければいけません.")
        elif conditions is None:
            pass

        # SQL文
        sql = 'select '

        def _select_clause_string(_column: str):
            """SELECT節の文字列を生成する."""
            if _column == '*':
                # 全件SELECTの場合はダブルクォートで囲う必要はない.
                return _column
            elif _column.startswith('count('):
                return _column
            else:
                return '"{}"'.format(_column)

        # SELECT 節の生成.
        first = True
        for select_column in select_columns:
            if first:
                sql += _select_clause_string(select_column)
                first = False
            else:
                sql += ', ' + _select_clause_string(select_column)
        else:
            sql += ' from {} '.format(table_name)

        # WHERE節の生成.
        where_clause = self.create_condition_clause_string(conditions)
        sql += where_clause

        # SQL文を実行.
        self.debug("Execute following SQL statement: {}".format(sql))
        self.execute(sql)

    def insert(self,
               table_name: str,
               columns: tuple,
               values: tuple):
        """指定したテーブルへインサートを行う.

        :param table_name: テーブル名.
        :param columns: カラム名
        :param values: 値
        """
        if not isinstance(columns, tuple) or not isinstance(values, tuple):
            raise TypeError("columns と values は タプル型で渡さなければいけません.")

        # 節の生成.
        def _create_clause(vals: tuple, type: str) -> str:
            # type => VALUE または COLUMN

            _clause_string = '('
            for val in vals:
                if type == 'COLUMN':
                    _clause_string += '{}, '.format(self.sanitize_column(val))
                elif type == 'VALUE':
                    _clause_string += '{}, '.format(self.sanitize_value(val))
            _clause_string = _clause_string[:-2]
            _clause_string += ')'

        # SQL例: INSERT INTO <テーブル名> (<column>, ...)
        # VALUES (<value>, ...)

        # カラム節の生成.
        column_clause_string = _create_clause(columns, 'COLUMN')

        # VALUES節の生成.
        values_clause_string = '('
        values_params = tuple(('?' for i in range(len(columns))))
        values_clause_string += ', '.join(values_params)
        values_clause_string += ')'

        # SQL 文　生成.
        sql = 'insert into {0} {1} VALUES {2}'.format(table_name,
                                                      column_clause_string,
                                                      values_clause_string)

        # SQL文実行.
        self.debug("Execute following SQL statement: {}".format(sql))
        self.execute(sql, values)

    def update(self, table_name: str, columns: tuple, values: tuple,
               where_conds: OrderedDict=None):
        """updateを行う.

        :param table_name: テーブル名
        :param columns: カラム名
        :param values: 値
        """
        # SQL例 UPDATE <table_name> SET <set clause list> [<where clause>]
        # SET節の生成.
        set_clause_string = ''
        for column, value in list(zip(columns, values)):
            set_clause_string += '{0} = {1}, '.format(self.sanitize_column(column), self.sanitize_value(value))
        else:
            set_clause_string = set_clause_string[:-2]

        # WHERE節の生成.
        where_clause_string = ''
        if where_conds is None:
            pass
        elif not isinstance(where_conds, OrderedDict):
            raise TypeError("where_conds は OrderedDict型で渡さなければいけません.")
        else:
            where_clause_string += self.create_condition_clause_string(conditions=where_conds)

        # 実行 SQL文の生成.
        sql = 'update {0} set {1} {2}'.format(table_name, set_clause_string, where_clause_string)
        self.debug("Execute following SQL statement: {}".format(sql))
        # SQL文の実行.
        self.execute(sql)

    def sanitize_column(self, column):
        """カラム名の整形を行う."""
        return "'{}'".format(str(column))

    def sanitize_value(self, value):
        """値の整形を行う."""
        if value is None:
            return "''"
        else:
            return "'{}'".format(value)

    def create_condition_clause_string(self, conditions: OrderedDict) -> str:
        """条件節の文字列を生成して返す.

        :param conditions: 条件節 キーワードをキーとし、値としてその条件を持つOrderedDict
        :return: str
        """
        condition_clause = ''

        if conditions is None:
            return condition_clause
        else:
            for key, val in conditions.items():
                if key is None:
                    raise ValueError("辞書のキーが存在しません.")
                else:
                    condition_clause += '{} {}, '.format(key, val)
        # 末尾の ',' は不要なためスライス
        condition_clause = condition_clause[:-2]
        return condition_clause
