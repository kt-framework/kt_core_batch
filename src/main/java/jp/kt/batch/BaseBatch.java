package jp.kt.batch;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import jp.kt.db.DbConnectManager;
import jp.kt.exception.KtException;
import jp.kt.exception.KtWarningException;
import jp.kt.fileio.FileUtil;
import jp.kt.logger.ApplicationLogger;
import jp.kt.mail.MailSender;
import jp.kt.prop.KtProperties;
import jp.kt.tool.DateUtil;
import jp.kt.tool.StringUtil;
import jp.kt.tool.Validator;

/**
 * バッチ実行の基底クラス.
 *
 * @author tatsuya.kumon
 */
public abstract class BaseBatch {
	private ApplicationLogger logger;

	/** ロックファイルのディレクトリーキー */
	private static final String LOCK_FILE_DIR_KEY = "kt.core.batch.dir.lockFile";

	/** 実行記録ファイルのディレクトリーキー */
	private static final String RUN_STAMP_FILE_DIR_KEY = "kt.core.batch.dir.runStampFile";

	/** ロックファイル名 */
	private String lockFilename;

	/** 実行記録ファイル名 */
	private String runStampFilename;

	/** ロックファイルによる制御を行うかどうかのフラグ */
	private boolean isLockFileControl = false;

	/** 同一日動作制御を行うかどうかのフラグ */
	private boolean isRunForDayControl = false;

	/** エラーメールを送るかどうかのフラグ（デフォルトtrue） */
	private boolean isSendErrorMail = true;

	/** 実行時引数 */
	private final String[] args;

	/** バッチ実行プロセスのユニークID */
	private String uniqueId;

	/** バッチプロパティ */
	private static KtProperties ktProps = KtProperties.getInstance();

	/**
	 * コンストラクタ.<br>
	 * 実行時引数無し.
	 */
	public BaseBatch() {
		this(null);
	}

	/**
	 * コンストラクタ.<br>
	 * 実行時引数あり.
	 *
	 * @param args
	 *            実行時引数
	 */
	public BaseBatch(String[] args) {
		this.logger = new ApplicationLogger(getLoggerName(), this.getClass());
		this.lockFilename = this.getClass().getName() + ".lock";
		this.runStampFilename = this.getClass().getName() + ".runstamp";
		this.args = args;
		this.uniqueId = StringUtil.createRandomText(10);
	}

	/**
	 * ログ出力のためのlogger名.
	 *
	 * @return logger名
	 */
	protected abstract String getLoggerName();

	/**
	 * 開始終了などの標準ログを出力するかどうかのフラグを返す.<br>
	 * 必要に応じてオーバーライドする.
	 *
	 * @return trueなら標準ログを出力する.
	 */
	protected boolean isOutputBasicLog() {
		return true;
	}

	/**
	 * 接続先DBキーを返す.
	 * <p>
	 * DB接続する場合は、このメソッドをオーバーライドする.<br>
	 * ここで返す値は、batch.propertiesの下記4つのキーのプレフィックスのこと.<br>
	 * kt.core.batch.&lt;dbKey&gt;.db.connection.driver<br>
	 * kt.core.batch.&lt;dbKey&gt;.db.connection.url<br>
	 * kt.core.batch.&lt;dbKey&gt;.db.connection.user<br>
	 * kt.core.batch.&lt;dbKey&gt;.db.connection.password
	 * </p>
	 *
	 * @return 接続先DBキー
	 */
	protected String getDbKey() {
		// デフォルトはnullを返すのでDB接続しない
		return null;
	}

	/**
	 * ロックファイルによる制御を行う場合、実行する.<br>
	 * コンストラクタ内で実行すること.
	 */
	protected void activateLockFileControl() {
		this.isLockFileControl = true;
	}

	/**
	 * 同一日動作制御を行いたい場合、実行する.<br>
	 * 同一日に1回しか実行しないという制御.<br>
	 * コンストラクタ内で実行すること.
	 */
	protected void activateRunForDayControl() {
		this.isRunForDayControl = true;
	}

	/**
	 * エラーメールを送りたくない場合、実行する.<br>
	 * コンストラクタ内で実行すること.
	 */
	protected void setErrorMailNotSend() {
		this.isSendErrorMail = false;
	}

	/**
	 * メイン処理.
	 * <p>
	 * 処理中に例外が発生した場合（{@link KtWarningException}は除く）は、 その例外をラップした
	 * {@link RuntimeException}をthrowする
	 * </p>
	 */
	public final void execute() {
		// 万が一コンストラクタ以外で制御変数セットメソッドを実行されたことを
		// 考慮して、制御用クラス変数をコピー
		String dbKey = this.getDbKey();
		boolean isLockFileControl = this.isLockFileControl;
		boolean isRunForDayControl = this.isRunForDayControl;

		// 起動ログ
		long start = System.nanoTime();
		if (isOutputBasicLog()) {
			logger.infoLog("A016", "[" + uniqueId
					+ "]_/_/_/_/_/   START   _/_/_/_/_/");
		}

		RuntimeException re = null;

		Connection con = null;
		boolean isLockFileError = false;
		FileUtil lockFile = null;
		try {
			/*
			 * ロックファイルの存在チェックと作成
			 */
			if (isLockFileControl) {
				// ロックファイルのFileUtilオブジェクトの生成
				lockFile = new FileUtil(ktProps.getString(LOCK_FILE_DIR_KEY));
				// ロックファイルディレクトリの存在チェックを実施することで
				// netappのパスを認識させる
				lockFile.isDirectory();
				// ファイルのパスに移動
				lockFile.setNextPath(lockFilename);
				// ロックファイルの存在チェック
				// FileUtil#existFile()だと時間がかかるので、File#exists()で判定する
				if (new File(lockFile.getPath()).exists()) {
					// ロックファイルがあるということは処理途中なので終了する
					isLockFileError = true;
					throw new KtWarningException("A018", "ロックファイルがあるので処理を終了します");
				}
				// ロックファイルの作成
				lockFile.touch();
			}
			/*
			 * 同一日動作制御
			 */
			FileUtil runStampFile = null;
			if (isRunForDayControl) {
				// runStampファイルのFileUtilオブジェクト生成
				runStampFile = new FileUtil(
						ktProps.getString(RUN_STAMP_FILE_DIR_KEY));
				// runStampファイルディレクトリの存在チェックを実施することで
				// netappのパスを認識させる
				runStampFile.isDirectory();
				// ファイルのパスに移動
				runStampFile.setNextPath(runStampFilename);
				// runStampファイルの存在チェック
				// FileUtil#existFile()だと時間がかかるので、File#exists()で判定する
				if (new File(runStampFile.getPath()).exists()) {
					// runStampファイルがあるので最終更新日時を取得する
					Date lastUpdateDate = runStampFile.getLastModifiedDate();
					// 今日の0時0分0秒
					Date today = DateUtil.setZeroTime(new Date());
					// 最終更新日時が今日であれば処理終了
					if (lastUpdateDate != null && !lastUpdateDate.before(today)) {
						throw new KtWarningException("A019", "本日は処理済みなので終了します");
					}
				}
			}
			/*
			 * DB接続
			 */
			if (!Validator.isEmpty(dbKey)) {
				con = getDbConnection(dbKey);
			}
			/*
			 * 各アプリケーション毎の処理を行う
			 */
			execute(con);
			/*
			 * DBコミット
			 */
			if (!Validator.isEmpty(dbKey)) {
				con.commit();
				if (isOutputBasicLog()) {
					logger.infoLog("A020", "最終commit実行");
				}
			}
			/*
			 * 同一日動作制御が有効な場合、動作完了ファイルを作成する
			 */
			if (isRunForDayControl) {
				runStampFile.touch();
			}
		} catch (KtException e) {
			// KtException発生
			re = exceptionOperation(e.getCode(), e, con);
		} catch (SQLException e) {
			// SQLException発生
			re = exceptionOperation("A003", e, con);
		} catch (Exception e) {
			// その他のException発生
			re = exceptionOperation("A004", e, con);
		} catch (Error e) {
			// java.lang.Error発生
			re = exceptionOperation("A005", e, con);
		} finally {
			try {
				/*
				 * Connectionのclose
				 */
				if (con != null && !con.isClosed()) {
					con.close();
				}
				/*
				 * ロックファイルの削除
				 */
				// ロックファイルチェックで引っかかった場合は削除しない
				if (isLockFileControl && !isLockFileError) {
					if (lockFile.isFile()) {
						lockFile.delete();
					}
				}
			} catch (Exception e) {
				re = exceptionOperation("A008", e, con);
			}
		}
		// 終了ログ
		if (isOutputBasicLog()) {
			// ナノ秒で計測
			long nanosec = System.nanoTime() - start;
			// 日時分秒ミリ秒で算出
			long d = TimeUnit.NANOSECONDS.toDays(nanosec);
			long h = TimeUnit.NANOSECONDS.toHours(nanosec);
			long m = TimeUnit.NANOSECONDS.toMinutes(nanosec);
			long s = TimeUnit.NANOSECONDS.toSeconds(nanosec);
			long ms = TimeUnit.NANOSECONDS.toMillis(nanosec);
			// 文字列生成
			StringBuilder timeStr = new StringBuilder();
			if (d > 0) {
				// 日
				timeStr.append(d);
				timeStr.append("d ");
			}
			if (h > 0) {
				// 時
				timeStr.append(h % 24);
				timeStr.append("h ");
			}
			if (m > 0) {
				// 分
				timeStr.append(m % 60);
				timeStr.append("m ");
			}
			if (s > 0) {
				// 秒
				timeStr.append(s % 60);
				timeStr.append("s ");
			}
			if (s == 0) {
				// 1秒未満だった場合のみ、ミリ秒を出力
				// ミリ秒
				timeStr.append(ms % 1000);
				timeStr.append("ms");
			}
			logger.infoLog("A017", "[" + uniqueId
					+ "]_/_/_/_/_/   END   _/_/_/_/_/ [実行時間: "
					+ timeStr.toString().trim() + "]");
		}
		// RuntimeExceptionがセットされている場合はthrowする
		if (re != null) {
			throw re;
		}
	}

	/**
	 * DB接続を取得.
	 *
	 * @param dbKey
	 *            接続キー
	 * @return Connectionオブジェクト
	 * @throws Exception
	 */
	private Connection getDbConnection(String dbKey) throws Exception {
		// batch.propertiesから接続情報を取得
		String driver = ktProps.getString("kt.core.batch." + dbKey
				+ ".db.connection.driver");
		String url = ktProps.getString("kt.core.batch." + dbKey
				+ ".db.connection.url");
		String user = ktProps.getString("kt.core.batch." + dbKey
				+ ".db.connection.user");
		String password = ktProps.getString("kt.core.batch." + dbKey
				+ ".db.connection.password");
		// DB接続
		Connection con;
		for (int i = 0; true; i++) {
			try {
				con = DbConnectManager.createConnection(driver, url, user,
						password);
				getLogger()
						.infoLog(
								"A052",
								"DB接続成功 [" + (i + 1) + "回目] [" + user + "@"
										+ url + "]");
				// 接続が成功したらループから抜ける
				break;
			} catch (Exception e) {
				getLogger().warnLog(
						"A053",
						"DB接続失敗 [" + (i + 1) + "回目] [" + user + "@" + url
								+ "] [" + e.getClass().getName() + "]");
				// エラーの場合は0.5秒sleepしてリトライ（最大10回）
				if (i < 9) {
					// 9回目まではリトライ
					Thread.sleep(500);
				} else {
					// 10回目はそのままExceptionをthrow
					throw e;
				}
			}
		}
		return con;
	}

	/**
	 * ExceptionやErrorが発生したときの共通処理.
	 *
	 * @param errorCode
	 *            エラーコード
	 * @param e
	 *            {@link Throwable}オブジェクト
	 * @param con
	 *            DB接続
	 * @return エラーの場合のみ、元のThrowableをラップしたRuntimeExceptionを返す
	 */
	private RuntimeException exceptionOperation(String errorCode, Throwable e,
			Connection con) {
		RuntimeException re = null;
		// ログ出力
		if (e instanceof KtWarningException) {
			logger.warnLog(errorCode, e.getMessage());
		} else {
			logger.errorLog(errorCode, e.getMessage(), e);
			re = new RuntimeException(e);
			// エラーの場合はエラー発生メールを送る
			if (isSendErrorMail) {
				try {
					MailSender mail = new MailSender();
					// 件名と本文作成
					String subject = ktProps
							.getString("kt.core.batch.errormail.subject");
					int index = getClass().getName().lastIndexOf(".");
					String className = (index >= 0) ? getClass().getName()
							.substring(index + 1) : getClass().getName();
					subject = StringUtil.replaceAll(subject, "#CLASS_NAME#",
							className);
					mail.setSubject(subject);
					// 本文
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					e.printStackTrace(pw);
					StringBuffer body = sw.getBuffer();
					mail.setBody(body.toString());
					// 送信先
					boolean isSend = false;
					for (int i = 1; true; i++) {
						String key = "kt.core.batch.errormail.to." + i;
						if (!ktProps.existKey(key)) {
							break;
						}
						String to = ktProps.getString(key);
						if (Validator.isEmpty(to)) {
							break;
						}
						mail.addToAddress(to);
						// ひとつでもtoがあれば送信する
						isSend = true;
					}
					// 返信先
					for (int i = 1; true; i++) {
						String key = "kt.core.batch.errormail.replyto." + i;
						if (!ktProps.existKey(key)) {
							break;
						}
						String replyTo = ktProps.getString(key);
						if (Validator.isEmpty(replyTo)) {
							break;
						}
						mail.addReplyToAddress(replyTo);
					}
					// 送信実行
					if (isSend) {
						mail.send();
					}
				} catch (Exception e2) {
					logger.errorLog("A021", "エラーメール送信でエラーが起きました", e2);
				}
			}
		}
		// DBトランザクションのロールバック
		try {
			if (con != null && !con.isClosed()) {
				con.rollback();
			}
		} catch (SQLException e3) {
			logger.errorLog("A007", "rollback処理時にエラーが起きました", e3);
			re = new RuntimeException(e);
		}
		return re;
	}

	/**
	 * ApplicationLoggerを取得する.
	 *
	 * @return {@link ApplicationLogger}オブジェクト
	 */
	protected ApplicationLogger getLogger() {
		return logger;
	}

	/**
	 * 実行時引数を返す.
	 *
	 * @return 実行時引数
	 */
	protected String[] getArgs() {
		return args;
	}

	/**
	 * 各アプリケーション毎の処理.
	 *
	 * @param con
	 *            DB接続
	 * @throws Exception
	 *             各バッチアプリケーションで例外発生した場合
	 */
	protected abstract void execute(Connection con) throws Exception;
}
