package com.jike.lhh.hbase;

import com.google.common.base.Objects;

/**
 * 学生数据存储对象
 *
 * @author lianghuahuang
 * @date 2021/7/27
 **/
public class StudentDTO {

    private String rowKey;

    private String name;

    private Info info = new Info();

    private Score score = new Score();

    public StudentDTO(){

    }

    public StudentDTO(String rowKey,String name,String studentId,String clazz,String understanding,String programming){
        this.rowKey=rowKey;
        this.name =name;
        this.info.clazz = clazz;
        this.info.studentId =studentId;
        this.score.understanding = understanding;
        this.score.programming = programming;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    public Score getScore() {
        return score;
    }

    public void setScore(Score score) {
        this.score = score;
    }

    class Info{

        private String studentId;

        private String clazz;

        public String getStudentId() {
            return studentId;
        }

        public void setStudentId(String studentId) {
            this.studentId = studentId;
        }

        public String getClazz() {
            return clazz;
        }

        public void setClazz(String clazz) {
            this.clazz = clazz;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("studentId", studentId)
                    .add("clazz", clazz)
                    .toString();
        }
    }

    class Score{

        private String understanding;

        private String programming;

        public String getUnderstanding() {
            return understanding;
        }

        public void setUnderstanding(String understanding) {
            this.understanding = understanding;
        }

        public String getProgramming() {
            return programming;
        }

        public void setProgramming(String programming) {
            this.programming = programming;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("understanding", understanding)
                    .add("programming", programming)
                    .toString();
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("rowKey", rowKey)
                .add("name", name)
                .add("info", info)
                .add("score", score)
                .toString();
    }
}

