package com.net7.scre.processors.authors;

import java.time.Year;

public class YearRange {

    private long lower;
    private long greater;

    public YearRange(long lower, long greater){
        this.lower = lower;
        this.greater = greater;
    }

    public YearRange(int lower, int greater){
        this.lower = lower;
        this.greater = greater;
    }

    public void updateYear(YearRange year){
        this.lower = Math.min(this.lower, year.getLower());
        this.greater = Math.max(this.greater, year.getGreater());
    }

    public long getLower(){
        return lower;
    }

    public long getGreater(){
        return greater;
    }
}
