package example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AddWordRequest {

    private String word;
    private String translation;

    @JsonCreator
    public AddWordRequest(@JsonProperty(value = "word", required = true) String word,
                          @JsonProperty(value = "translation", required = false) String translation) {
        this.word = word;
        this.translation = translation;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getTranslation() {
        return translation;
    }

    public void setTranslation(String translation) {
        this.translation = translation;
    }





}
