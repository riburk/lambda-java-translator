package example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VocabularyEvent {

    private String eventType;
    private String word;

    @JsonCreator
    public VocabularyEvent(@JsonProperty(value = "eventType", required = true) String eventType,
                          @JsonProperty(value = "word", required = true) String word) {
        this.eventType = eventType;
        this.word = word;
    }







}
