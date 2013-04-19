package storm.starter;

import java.util.List;

import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Kestrelからメッセージを取得し、ワードカウントを行うTopology。<br>
 * 動作は下記の通り。<br>
 * <ol>
 * <li>KestrelThriftSpoutにてSentenceを受信する</li>
 * <li>SplitSentenceBoltにて単語単位に分割し、単語単位でグルーピングしてWordCountに送信する</li>
 * <li>WordCountBoltにて単語をカウントする</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>Kestrel.ImmediateAck : Kestrelに対してすぐAckを返すかの設定</li>
 * <li>TupleEmit.Interval : Kestrelから取得したTupleをemitするインターバル</li>
 * <li>Kestrel.Hosts : Kestrelが配置されるホスト:Portの配列</li>
 * <li>Kestrel.QueueName : Kestrelのキュー名称</li>
 * <li>KestrelSpout.Parallelism : KestrelThriftSpoutの並列度</li>
 * <li>SplitSentence.Parallelism : SplitSentenceの並列度</li>
 * <li>WordCount.Parallelism : WordCountの並列度</li>
 * </ul>
 * 
 * ローカル環境での実行方法<br>
 * MessageTopologyにKestrelのアドレスを指定し、<br>
 * storm-starterプロジェクト　クラス「storm.starter.WordCountTopology」を下記の条件で実行する。
 * <ol>
 * <li>Program arguments : conf/WordCountTopology.yaml true</li>
 * </ol>
 * 
 * @author acromusashi
 */
public class WordCountTopology
{
    /**
     * インスタンス化防止のためのコンストラクタ
     */
    private WordCountTopology()
    {}

    /**
     * プログラムエントリポイント<br/>
     * <ul>
     * <li>起動引数:arg[0] 設定値を記述したyamlファイルパス</li>
     * <li>起動引数:arg[1] Stormの起動モード(true:LocalMode、false:DistributeMode)</li>
     * </ul>
     * 
     * @param args
     *            起動引数
     * @throws Exception
     *             初期化例外発生時
     */
    public static void main(String[] args) throws Exception
    {
        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // 設定値オブジェクトから設定値を取得
        boolean immediateAck = Boolean.valueOf(StormConfigUtil.getStringValue(
                conf, "Kestrel.ImmediateAck", "false"));
        int interval = StormConfigUtil.getIntValue(conf, "TupleEmit.Interval",
                1);
        List<String> kestrelHosts = StormConfigUtil.getStringListValue(conf,
                "Kestrel.Hosts");
        String kestrelQueueName = StormConfigUtil.getStringValue(conf,
                "Kestrel.QueueName", "MessageQueue");
        int kestrelSpoutPara = StormConfigUtil.getIntValue(conf,
                "KestrelSpout.Parallelism", 1);
        int splitSentencePara = StormConfigUtil.getIntValue(conf,
                "SplitSentence.Parallelism", 1);
        int wordCountPara = StormConfigUtil.getIntValue(conf,
                "WordCount.Parallelism", 1);

        // Topologyを作成する
        TopologyBuilder builder = new TopologyBuilder();

        // Topology Setting
        // Add Spout(KestrelThriftSpout)
        KestrelThriftSpout kestrelSpout = new KestrelThriftSpout(kestrelHosts,
                kestrelQueueName, new StringScheme());
        kestrelSpout.setImmediateAck(immediateAck);
        kestrelSpout.setTupleEmitInterval(interval);
        builder.setSpout("KestrelSpout", kestrelSpout, kestrelSpoutPara);

        // Add Bolt(KestrelThriftSpout -> SplitSentence)
        builder.setBolt("SplitSentence", new SplitSentenceBolt(),
                splitSentencePara).shuffleGrouping("KestrelSpout");

        // Add Bolt(SplitSentence -> WordCount)
        builder.setBolt("WordCount", new WordCountBolt(), wordCountPara).fieldsGrouping(
                "SplitSentence", new Fields("word"));

        conf.setDebug(true);

        if (isLocal)
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCount", conf, builder.createTopology());
        }
        else
        {
            StormSubmitter.submitTopology("WordCountTopology", conf,
                    builder.createTopology());
        }
    }
}
