package cn.lehome.dispatcher.quartz.exception;

public class ScheduleException extends RuntimeException {

    private static final long serialVersionUID = -1921648378954132894L;

    public ScheduleException(Throwable e) {
        super(e);
    }

    public ScheduleException(String message) {
        super(message);
    }
}
