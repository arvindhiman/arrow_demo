import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class UtilTest {

    public static void main(String[] args) throws JsonProcessingException {
        String fact = "DailyGL.ABX";

        String[] tokens = fact.split("\\.");
        System.out.println(tokens[0] + " " + tokens[1]);

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("Dataset", "Positions");
        requestMap.put("Funds", "123");
        requestMap.put("Facts", new String[] {"DailyGL", "TotalReturn"});

        try {
            System.out.println(objectMapper.writeValueAsString(requestMap));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
