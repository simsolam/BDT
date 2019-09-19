package jpt;

/*
 * Copyright (c) Ian F. Darwin, http://www.darwinsys.com/, 1996-2002.
 * All rights reserved. Software written by Ian F. Darwin and others.
 * $Id: LICENSE,v 1.8 2004/02/09 03:33:38 ian Exp $
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS''
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * Java, the Duke mascot, and all variants of Sun's Java "steaming coffee
 * cup" logo are trademarks of Sun Microsystems. Sun's, and James Gosling's,
 * pioneering role in inventing and promulgating (and standardizing) the Java
 * language and environment is gratefully acknowledged.
 *
 * The pioneering role of Dennis Ritchie and Bjarne Stroustrup, of AT&T, for
 * inventing predecessor languages C and C++ is also gratefully acknowledged.
 */
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.Date;
import java.util.regex.*;

/**
 * Common fields for Apache Log demo.
 */
interface LogExample {
    /** The number of fields that must be found. */
    public static final int NUM_FIELDS = 7;

    /** The sample log entry to be parsed. */

}
enum field{
    IP, DateTime, Request, Response, BytesSent, Referer, Browser;
}

/**
 * Parse an Apache log file with Regular Expressions
 */
public class LogReg implements LogExample {
    static String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d-]+)";// \"([^\"]+)\" \"([^\"]+)\"";
    static String logEntryLine = "";
    //"123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";

    private LogReg(String args){
        logEntryLine = args;
    }

    public static LogReg readLine(String args) {
        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(args);
        if (!matcher.matches() ||
                NUM_FIELDS != matcher.groupCount()) {
            System.err.println("Bad log entry (or problem with RE?):");
            System.err.println(logEntryLine);
            return null;
        }
        return new LogReg(args);
    }

    public String get(field f){
        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logEntryLine);
        if (!matcher.matches() ||
                NUM_FIELDS != matcher.groupCount()) {
            System.err.println("Bad log entry (or problem with RE?):");
            System.err.println(logEntryLine);
            return null;
        }

        String data = "";
        switch (f){
            case IP:
                data = matcher.group(1);
                break;
            case DateTime:
                data = matcher.group(4);
                break;
            case Request:
                data = matcher.group(5);
                break;
            case Response:
                data = matcher.group(6);
                break;
            case BytesSent:
                data = matcher.group(7);
                break;
            case Referer:
                if(!matcher.group(8).equals("-"))
                    data = matcher.group(8);
                break;
            case Browser:
                data = matcher.group(9);
                break;
        }
        System.out.println(f.toString()+":"+data);
        return data;
    }

    public Date getLogDate() throws ParseException {
        System.out.println(get(field.DateTime));
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss");
        return sdf.parse(get(field.DateTime).split(" ")[0]);
    }

    public String getUrl() {
        String[] request = get(field.Request).split(" ");
        return request[1];
    }
}
