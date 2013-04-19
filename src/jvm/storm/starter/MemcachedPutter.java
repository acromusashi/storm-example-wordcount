package storm.starter;

import java.util.Random;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

/**
 * Memcachedプロトコルを用いてKestrelにデータの投入を行うプログラム
 * 
 * @author acromusashi
 */
public class MemcachedPutter
{
    /** 投入メッセージ一覧 */
    static String[] sentences__ = new String[]
                                {
            "Massachusetts Gov Deval Patrick said he shared the frustration that the person or people responsible",
            "It's going to happen by doing the careful work that must be done in a thorough investigation Patrick",
            "That means going through the couple of blocks at the blast scene square inch by square inch and inch",
            "The bombs were crudely fashioned from ordinary kitchen pressure cookers packed with explosives nails" };

    /**
     * インスタンス化防止のためのコンストラクタ
     */
    private MemcachedPutter()
    {}

    /**
     * プログラムエントリポイント<br/>
     * <ul>
     * <li>起動引数:arg[0] KestrelMemcachedポート接続先(例：192.168.100.100:22133)</li>
     * <li>起動引数:arg[1] Queue名称(例：MessageQueue)</li>
     * <li>起動引数:arg[2] 送信メッセージ数(例：192.168.100.100:22133)</li>
     * </ul>
     * 
     * @param args
     *            起動引数
     */
    public static void main(String[] args)
    {
        // Memcachedの接続プール初期化
        String[] servers =
        { args[0] };
        String queueName = args[1];
        SockIOPool pool = SockIOPool.getInstance();
        pool.setServers(servers);

        pool.initialize();

        // 初期化された接続プールに紐づけられたMemCachedClientを生成
        MemCachedClient mcc = new MemCachedClient();

        // 文章一覧からランダムで取得するための乱数機生成
        int putCount = Integer.valueOf(args[2]);
        Random random = new Random();
        int sentenceNum = sentences__.length;

        // 送信メッセージ数の数だけランダムで文章一覧から取得し、Memchachedプロトコルで送信
        for (int count = 0; count < putCount; count++)
        {
            int sentenceIndex = random.nextInt(sentenceNum);
            String putSentence = sentences__[sentenceIndex];
            mcc.set(queueName, putSentence);
        }
    }
}
