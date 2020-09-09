package com.xiaoying.common.mq.support;

import com.dianping.cat.Cat;
import com.dianping.cat.message.ForkableTransaction;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: misskey
 * @Date: 2019-05-16
 * @Version 1.0
 */
public class CatTranscation implements Transaction, AutoCloseable {


    private Transaction transaction;

    public CatTranscation(String type, String name) {
        try {
            transaction = Cat.newTransaction(type, name);
        } catch (Throwable throwable) {
            // ignore
        }
    }

    @Override
    public Transaction addChild(Message message) {
        if (transaction != null) {
            try {
                return transaction.addChild(message);
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return null;
    }

    @Override
    public ForkableTransaction forFork() {
        if (transaction != null) {
            try {
                return transaction.forFork();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return null;
    }

    @Override
    public List<Message> getChildren() {
        if (transaction != null) {
            try {
                return transaction.getChildren();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return new ArrayList<>(0);
    }

    @Override
    public long getDurationInMicros() {

        if (transaction != null) {
            try {
                return transaction.getDurationInMicros();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return 0;
    }

    @Override
    public long getDurationInMillis() {
        if (transaction != null) {
            try {
                return transaction.getDurationInMillis();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return 0;
    }

    @Override
    public long getRawDurationInMicros() {
        if (transaction != null) {
            try {
                return transaction.getRawDurationInMicros();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return 0;
    }

    @Override
    public boolean hasChildren() {
        if (transaction != null) {
            try {
                return transaction.hasChildren();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return false;
    }

    @Override
    public void setDurationInMicros(long l) {
        if (transaction != null) {
            try {
                transaction.setDurationInMicros(l);
            } catch (Throwable throwable) {
                // ignore
            }
        }
    }

    @Override
    public void setDurationInMillis(long l) {
        if (transaction != null) {
            try {
                transaction.setDurationInMillis(l);
            } catch (Throwable throwable) {
                // ignore
            }
        }
    }

    @Override
    public void setDurationStart(long l) {
        if (transaction != null) {
            try {
                transaction.setDurationStart(l);
            } catch (Throwable throwable) {
                // ignore
            }
        }
    }

    @Override
    public void addData(String s) {
        if (transaction != null) {
            try {
                transaction.addData(s);
            } catch (Throwable throwable) {
                // ignore
            }
        }
    }

    @Override
    public void addData(String s, Object o) {
        if (transaction != null) {
            try {
                transaction.addData(s, o);
            } catch (Throwable throwable) {
                // ignore
            }
        }
    }

    public void addEvent(String type, String name) {
        try {
            Cat.logEvent(type, name);
        } catch (Throwable ex) {
            // ignore
        }
    }


    public void logErrorWithCategory(String category, Throwable throwable) {
        try {
            Cat.logErrorWithCategory(category, throwable);
        } catch (Throwable ex) {
            // ignore
        }
    }


    public void logErrorWithCategory(String category, String message, Throwable throwable) {
        try {
            Cat.logErrorWithCategory(category, message, throwable);
        } catch (Throwable ex) {
            // ignore
        }
    }

    @Override
    public void complete() {
        if (transaction != null) {
            try {
                transaction.complete();
            } catch (Throwable throwable) {
                // ignore
            }
        }
    }

    @Override
    public Object getData() {
        if (transaction != null) {
            try {
                return transaction.getData();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return null;
    }

    @Override
    public String getName() {
        if (transaction != null) {
            try {
                return transaction.getName();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return null;
    }

    @Override
    public String getStatus() {
        if (transaction != null) {
            try {
                return transaction.getStatus();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return null;
    }

    @Override
    public long getTimestamp() {
        if (transaction != null) {
            try {
                return transaction.getTimestamp();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return 0;
    }

    @Override
    public String getType() {
        if (transaction != null) {
            try {
                return transaction.getType();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return null;
    }

    @Override
    public boolean isCompleted() {
        if (transaction != null) {
            try {
                return transaction.isCompleted();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return false;
    }

    @Override
    public boolean isSuccess() {
        if (transaction != null) {
            try {
                return transaction.isSuccess();
            } catch (Throwable throwable) {
                // ignore
            }
        }
        return false;
    }

    @Override
    public void setStatus(String s) {
        if (transaction != null) {
            try {
                transaction.setStatus(s);
            } catch (Throwable throwable) {
                // ignore
            }
        }
    }

    @Override
    public void setStatus(Throwable throwable) {
        if (transaction != null) {

            try {
                transaction.setStatus(throwable);
            } catch (Throwable ex) {
                // ignore
            }
        }
    }

    @Override
    public void setSuccessStatus() {
        if (transaction != null) {
            try {
                transaction.setSuccessStatus();
            } catch (Throwable ex) {

            }
        }
    }

    @Override
    public void setTimestamp(long timestamp) {
        try {
            transaction.setTimestamp(timestamp);
        } catch (Throwable ex) {
            // ignore
        }
    }


    @Override
    public void close() throws Exception {
        if (transaction != null) {
            transaction.complete();
        }
    }
}
