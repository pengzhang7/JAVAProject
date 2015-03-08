/**
 * Created by blice on 2015/3/8.
 */
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

import static  org.elasticsearch.node.NodeBuilder.*;

public class testES {
    private Client client;

    public void init(){
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost",9300));
    }

    public void close(){
        client.close();
    }

    /**
     * 转换成json对象
     *
     * @param user
     * @return
     */
    private String generateJson(User user) {
        String json = "";
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                    .startObject();
            contentBuilder.field("id", user.getId() + "");
            contentBuilder.field("name", user.getName());
            contentBuilder.field("age", user.getAge() + "");
            json = contentBuilder.endObject().string();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json;
    }


    /*
    * index
    * */
    public void creatIndex(){
        for (int i = 0;i<1000;i++){
            User user = new User();

            user.setId(new Long(i));
            user.setName("huang fox "+i);
            user.setAge(i%100);

            client.prepareIndex("users","user").setSource(generateJson(user)).execute().actionGet();
        }
    }

    /**
     * search
     * */
    public void search(){
        SearchResponse response = client.prepareSearch("users").
                setTypes("user").
                setSearchType(SearchType.DFS_QUERY_AND_FETCH).
                setQuery(QueryBuilders.termQuery("name", "fox")).
                setPostFilter(FilterBuilders.rangeFilter("age").from(20).to(40)).setFrom(0).setSize(60).addSort("id", SortOrder.DESC).setExplain(true).execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        for (int i=0;i<hits.getHits().length;i++){
            System.out.println(hits.getHits()[i].getSourceAsString());
        }
    }

     public static void main(String[] args){
        testES client = new testES();
        client.init();
//        client.creatIndex();
         client.search();
        client.close();
    }
}
