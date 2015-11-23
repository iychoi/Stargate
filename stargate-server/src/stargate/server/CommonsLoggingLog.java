/*
 * The MIT License
 *
 * Copyright 2015 iychoi.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package stargate.server;

import org.eclipse.jetty.util.log.AbstractLogger;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/**
 *
 * @author iychoi
 */
public class CommonsLoggingLog extends AbstractLogger {
    private org.apache.commons.logging.Log _logger;
    private String _name;
    private boolean _debugEnalbed;
    
    public CommonsLoggingLog() {
        this(CommonsLoggingLog.class.getName());
    }

    public CommonsLoggingLog(String name) {
        this._logger = org.apache.commons.logging.LogFactory.getLog(name);
        this._name = name;
    }

    @Override
    public String getName() {
        return this._name;
    }

    @Override
    public void warn(String msg, Object... args) {
        this._logger.warn(format(msg, args));
    }

    @Override
    public void warn(Throwable thrown) {
        this._logger.warn(thrown);
    }

    @Override
    public void warn(String msg, Throwable thrown) {
        this._logger.warn(msg, thrown);
    }

    @Override
    public void info(String msg, Object... args) {
        this._logger.info(format(msg, args));
    }

    @Override
    public void info(Throwable thrown) {
        this._logger.info(thrown);
    }

    @Override
    public void info(String msg, Throwable thrown) {
        this._logger.info(msg, thrown);
    }

    @Override
    public boolean isDebugEnabled() {
        return this._debugEnalbed;
    }

    @Override
    public void setDebugEnabled(boolean enabled) {
        this._debugEnalbed = enabled;
    }

    @Override
    public void debug(String msg, Object... args) {
        if(this._debugEnalbed) {
            this._logger.debug(format(msg, args));
        }
    }

    @Override
    public void debug(String msg, long arg) {
        if(this._debugEnalbed) {
            this._logger.debug(format(msg, arg));
        }
    }

    @Override
    public void debug(Throwable thrown) {
        if(this._debugEnalbed) {
            this._logger.debug(thrown);
        }
    }

    @Override
    public void debug(String msg, Throwable thrown) {
        if(this._debugEnalbed) {
            this._logger.debug(msg, thrown);
        }
    }

    @Override
    protected Logger newLogger(String fullname) {
        return new CommonsLoggingLog(fullname);
    }

    @Override
    public void ignore(Throwable ignored) {
        this._logger.warn(Log.IGNORED, ignored);
    }

    private String format(String msg, Object... args) {
        msg = String.valueOf(msg);
        String braces = "{}";
        StringBuilder builder = new StringBuilder();
        int start = 0;
        for (Object arg : args) {
            int bracesIndex = msg.indexOf(braces, start);
            if (bracesIndex < 0) {
                builder.append(msg.substring(start));
                builder.append(" ");
                builder.append(arg);
                start = msg.length();
            } else {
                builder.append(msg.substring(start, bracesIndex));
                builder.append(String.valueOf(arg));
                start = bracesIndex + braces.length();
            }
        }
        builder.append(msg.substring(start));
        return builder.toString();
    }
}
