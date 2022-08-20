package com.bytetenns.client.tools;


import org.jline.builtins.Completers;
import org.jline.reader.*;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import com.bytetenns.client.tools.command.Command;
import com.bytetenns.client.tools.command.CommandFactory;

import java.io.File;
import java.io.IOException;

/**
 * 命令Reader
 *
 * @author Sun Dasheng
 */
public class CommandReader {
    private String host;
    private String currentPath = "/";
    private LineReader lineReader;

    public CommandReader(String host) throws IOException {
        this.host = host;
        Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build();
        StringsCompleter stringsCompleter = new StringsCompleter();
        Completer getCompleter = new ArgumentCompleter(  // 代码补全
                new StringsCompleter("get"),
                NullCompleter.INSTANCE,
                new Completers.FileNameCompleter(),
                NullCompleter.INSTANCE
        );

        Completer putCompleter = new ArgumentCompleter(
                new StringsCompleter("PUT"),
                new Completers.FileNameCompleter(),
                NullCompleter.INSTANCE
        );

        Completer completer = new AggregateCompleter(
                getCompleter,
                putCompleter
        );
        this.lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(stringsCompleter)
                .completer(completer)
                .build();
    }

    public Command readCommand() {
        try {
            String command = lineReader.readLine("namenode(" + host + ") (" + currentPath + ") > ");
            return CommandFactory.getCommand(command, currentPath, this);
        } catch (UserInterruptException e) {
            System.exit(0);
            return null;
        } catch (EndOfFileException e) {
            System.out.println("\nBye.");
            System.exit(0);
            return null;
        }
    }

    public void setCurrentPath(String currentPath) {
        this.currentPath = currentPath;
    }

    public LineReader getLineReader() {
        return lineReader;
    }
}
