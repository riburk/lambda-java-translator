package Translation;

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


    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
