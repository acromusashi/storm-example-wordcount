package storm.starter;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 受信した英語の文章を単語単位に分割して次のBoltに送信するBolt
 * <ul>
 * <li>インプット 文章 例)「How are you」</li>
 * <li>Boltの処理 受信メッセージに含まれる文章を単語単位に分割する</li>
 * <li>アウトプット 単語単位に分割されたTuple「How」「are」「you」</li>
 * </ul>
 * 
 * @author acromusashi
 */
public class SplitSentenceBolt extends BaseRichBolt implements IRichBolt
{
    /** serialVersionUID */
    private static final long         serialVersionUID = 4721351466208306580L;

    /** メッセージを送信するコレクタオブジェクト */
    private transient OutputCollector collector;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public SplitSentenceBolt()
    {}

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector)
    {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)
    {
        // 文章を単語単位に分割する
        String sentence = input.getStringByField("str");
        String[] words = StringUtils.split(sentence);

        // 単語単位にTupleに分割し、次のBoltに送信する
        for (String targetWord : words)
        {
            this.collector.emit(input, new Values(targetWord));
        }

        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word"));
    }
}
