package storm.starter;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 受信した単語の出現回数をカウントするBolt
 * <ul>
 * <li>インプット 単語 例)「How」</li>
 * <li>Boltの処理 単語の出現回数をカウントする</li>
 * <li>アウトプット 単語と出現回数 例）「How 5」</li>
 * </ul>
 * 
 * @author acromusashi
 */
public class WordCountBolt extends BaseBasicBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 9080948772140456741L;

    /** 単語出現回数カウンタ */
    Map<String, Integer>      counts           = new HashMap<String, Integer>();

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public WordCountBolt()
    {}

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        // 単語出現回数カウンタからカウンタを取得
        String word = input.getStringByField("word");
        Integer count = this.counts.get(word);

        // カウンタ値が存在しない場合は値を0で初期化
        if (count == null)
        {
            count = 0;
        }

        count++;

        // 結果を単語出現回数カウンタに反映
        this.counts.put(word, count);

        // 単語と出現回数をペアにして次のBoltに送信
        collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "count"));
    }
}
