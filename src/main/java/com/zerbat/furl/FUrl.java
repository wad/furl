package com.zerbat.furl;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Normalizes URLs
 * This code is heavily optimized for performance under hadoop, avoiding allocation of memory on the heap.
 * It sacrifices some clarity for performance.
 * <p/>
 * This code is suitable and optimized for easy assimilation into pure map-reduce code.
 * <p/>
 * Assumption: In the case of a URL like this: "abc.com/qqq/abc"
 * It is ambiguous whether or not "abc" is the document or is a directory. Since we cannot tell (the web server
 * will do the right thing either way, because it has access to the file system) we shall assume that it is a
 * file, and not a directory, and act accordingly. That means that if we're in omitDocumentUntilEnd mode, that
 * it will be omitted. It also means that we will not be appending a trailing slash to the end of the path segment,
 * unless there is no path present.
 * <p/>
 * NOTE: Only the first "www." will be chopped. So "www.www.www.facebook.com" normalizes to "www.www.facebook.com".
 *
 * @author Eric Wadsworth
 */
public class FUrl {

    // Holds the original URL that was scanned.
    String originalUrl;

    // This buffer is used to store the scanned URL string.
    char[] urlBuffer;

    // This buffer is used to store the assembled URL
    char[] assemblyBuffer;

    // 2k is the limit of IE6, so it's probably good for our use here.
    static final int DEFAULT_URL_BUFFER_SIZE = 2048;
    static final int DEFAULT_ASSEMBLY_BUFFER_SIZE = 2048;

    int urlBufferSize;
    int assemblyBufferSize;

    // this array holds indexes into buf, and how long each section is.
    int[][] offsets;

    // description of the two columns of the array.
    static final int INDEX = 0;
    static final int LENGTH = 1;
    static final int MAX_COLUMNS = 2;

    /**
     * First part of a url, something like "http". It doesn't include the "://" part.
     */
    static final int SCHEME = 0;

    /**
     * also the domain name, or the IP address, something like "www.example.com".
     */
    static final int HOSTNAME = 1;

    /**
     * always a number, like "80", "443" or "8080". Is not preceeded here by ':', though that is what distinguishes it.
     */
    static final int PORT = 2;

    /**
     * this specifies where the document is, and includes the document. It always starts with '/' though that is
     * not included here.
     * "some/path/to/doc.html"
     * "some/path/to/doc"
     * "" (note that the '/' is implied here)
     * "doc.html"
     * "index.html"
     */
    static final int PATH = 3;

    /**
     * starts with '?' though that is not included here. Subsequent query parts are separated by '&' characters.
     * Example: "abc=def&ppp=rrr" (note that the preceeding '?' is assumed.)
     */
    static final int QUERY = 4;

    /**
     * starts with '#', and is the last part of a URL. The '#' character is omitted here.
     * Example: "anchorIsHere" (the preceeding '#' is implied.)
     */
    static final int ANCHOR = 5;

    // This is one more than the ANCHOR index, or whatever the last index is above.
    static final int MAX_POSITIONS_NOTED = 6;

    // This is used in the positionIndexesAndLengths array to indicate that something was not yet visited.
    static final int NOT_VISITED = -1;

    // When assembling a return value, many methods share usage of this member variable.
    // It keeps track of where in the assembly buffer stuff should be added.
    int assemblyBufferIndex;

    // this is the index into the url string
    int urlIndex = 0;

    // this is the index into the urlBuffer. We start at zero.
    int urlBufferIndex = 0;

    // the segment advances through the various parts of the URL,
    // from SCHEME to HOSTNAME to PORT to PATH to QUERY.
    // For example, take http://example.com:80/somedir/index.html?abc=def&ghi=jkl#qqq=rrr
    // SCHEME=http
    // HOSTNAME=example.com
    // PORT=80
    // PATH=/somedir/index.html
    // QUERY=?abc=def&ghi=jkl
    // ANCHOR=#qqq=rrr
    int currentSegment = SCHEME;

    // this flag is TRUE to keep going, false otherwise.
    boolean keepScanningUrl = true;

    /**
     * This constructor applies the default buffer sizes
     */
    public FUrl() {
        this(DEFAULT_URL_BUFFER_SIZE, DEFAULT_ASSEMBLY_BUFFER_SIZE);
    }

    /**
     * This constructor is used for test instrumentation purposes. Much easier to do boundary testing
     * on absurdly tiny buffers than the normal large ones.
     *
     * @param urlBufferSize      specified here for testing
     * @param assemblyBufferSize specified here for testing
     */
    FUrl(int urlBufferSize, int assemblyBufferSize) {
        this.urlBufferSize = urlBufferSize;
        this.assemblyBufferSize = assemblyBufferSize;

        // allocate memory for the buffers and set of positions and indexes
        urlBuffer = new char[getUrlBufferSize()];
        assemblyBuffer = new char[getAssemblyBufferSize()];
        offsets = new int[MAX_POSITIONS_NOTED][MAX_COLUMNS];
    }

    /**
     * This will parse a URL and identify the various segments. This must be called before the user
     * can obtain a normalized or filtered URL. This will cut out any whitespace (other than newlines).
     *
     * @param rawUrl the URL to be normalized
     * @return a reference to this object itself, so that you can scan and get on the same line in calling code.
     * @throws IOException if needed
     */
    public FUrl scanUrl(String rawUrl) throws IOException {
        return scanUrl(rawUrl, false);
    }

    /**
     * This will parse a URL and identify the various segments. This must be called before the user
     * can obtain a normalized or filtered URL.
     *
     * @param rawUrl     the URL to be normalized
     * @param keepSpaces set this to TRUE if you want to retain spaces. They might be used in query params, for example. Tabs are converted to spaces.
     * @return a reference to this object itself, so that you can scan and get on the same line in calling code.
     * @throws IOException if needed
     */
    public FUrl scanUrl(String rawUrl, boolean keepSpaces) throws IOException {

        // keep the original URL, so that the method that asks if it should be discarded can use it.
        originalUrl = rawUrl;

        resetAllMarkers();

        // if there is nothing here, return success anyway. Subsequent methods will all still work, though
        // the results are unlikely to be interesting.
        if (originalUrl == null) {
            return this;
        } else {
            // store the original URL, but trimmed.
            if (!keepSpaces) {
                originalUrl = removeWhitespace(originalUrl);
            }

            // bail if there's nothing here after the trim
            if (originalUrl.length() == 0) {
                return this;
            }
        }

        // now go through the url, noting where the various segments of it are, and load it into the urlBuffer
        try {
            int size = originalUrl.length();

            urlIndex = 0;
            urlBufferIndex = 0;
            currentSegment = SCHEME;
            keepScanningUrl = true;

            while (keepScanningUrl && (urlIndex < size)) {

                // read the current character
                char c = originalUrl.charAt(urlIndex);

                if (keepSpaces) {
                    // convert tab to a space
                    if (c == '\t' || c == '\n' || c == '\r') {
                        c = ' ';
                    }
                } else {
                    // skip any whitespace, we don't care about it.
                    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                        urlIndex++;
                        continue;
                    }
                }

                // Take different actions depending on which segment we're currently looking for,
                switch (currentSegment) {

                    // for this segment, assume it's the scheme. Read characters until a non-alphanumeric character is encountered,
                    // check to see if it's a ':' and followed by "//". If so, then we correctly captured the scheme.
                    // if it's anything other than that, such as a '.' '/' or '-', then it must be the hostname: change the
                    // current segment to HOSTNAME and start over.
                    case SCHEME:
                        scanScheme(size, c);
                        break;

                    // For this segment, accept only alphanumeric characters, along with '.' and '-'. Once anything
                    // else is found, we must be out of the domain name.
                    case HOSTNAME:
                        scanHostname(size, c);
                        break;

                    // For this segment, accept only a ':' followed by numeric characters.
                    // If there is no ':', then assume there is no port.
                    case PORT:
                        scanPort(size, c);
                        break;

                    // The first character must either be '/' or '?'. If it's a '?', that means that there is no path
                    // specified, and we will proceed with the query segment.
                    // Once a '?' is discovered, that is the start of the optional query.
                    case PATH:
                        scanPath(size, c);
                        break;

                    // the first character must be '?', or it's not query, and we've incorrectly passed the end
                    // of the path segment.
                    case QUERY:
                        scanQuery(size, c);
                        break;

                    // the first character must be '#' or it's not an anchor, and we're incorrectly passed the end
                    // of the query segment.
                    case ANCHOR:
                        scanAnchor(size, c);
                        break;

                    // Unknown situation. Barf, it's a bug.
                    default:
                        throw new RuntimeException("Hit an unknown value for currentSegment: " + currentSegment);
                }
            }
        } catch (Exception e) {
            // return an error condition if something went wrong. Resetting the markers makes sure that no data
            // can be obtained after the run.
            resetAllMarkers();
            throw new IOException("Failed to scan url: [" + rawUrl + "]", e);
        }
        return this;
    }

    void scanScheme(int size, char c) {
        // check to see if the current character is valid for this segment
        if (isAlphanumeric(c)) {
            // check to see if we've started chomping through this segment of the url
            if (segmentNotFoundYet(SCHEME)) {
                // We're at the start of this segment, capture the index.
                offsets[SCHEME][INDEX] = urlBufferIndex;
            }
            // Capture the character into the buffer, and proceed on to the next character.
            urlBuffer[urlBufferIndex++] = toLower(c);
            urlIndex++;
        } else {
            // we are either at some trash at the start, or have reached the end of the scheme,
            // or there was no scheme specified, in which case we're probably in the hostname.
            if (segmentNotFoundYet(SCHEME)) {
                // we haven't even started on the scheme yet, but already we found odd characters.
                // give the hostname segment a shot at it.
                offsets[SCHEME][INDEX] = 0;
                offsets[SCHEME][LENGTH] = 0;
                currentSegment = HOSTNAME;
            } else {
                // at the end of the scheme, or this wasn't a scheme.

                // check the character we just saw
                if (c == ':') {
                    // check the next two characters also
                    if (urlIndex + 2 >= size) {
                        // there aren't enough remaining characters. Eject.
                        offsets[SCHEME][INDEX] = NOT_VISITED;
                        offsets[SCHEME][LENGTH] = NOT_VISITED;
                        keepScanningUrl = false;
                    } else {
                        // there are two more characters, but are they the expected ones?
                        if (originalUrl.charAt(urlIndex + 1) == '/' && originalUrl.charAt(urlIndex + 2) == '/') {
                            // the next two characters are "//", so it must have been the scheme.

                            // set the length of the scheme segment
                            offsets[SCHEME][LENGTH] = urlBufferIndex - offsets[SCHEME][INDEX];

                            // skip the '://'
                            urlIndex += 3;
                        } else {
                            // the next two characters weren't "//", so it must not have been the scheme.
                            // It was probably the hostname (the ':' preceeds the port)
                            // Start over, checking for hostname instead of scheme.
                            offsets[SCHEME][INDEX] = NOT_VISITED;
                            urlIndex = 0;
                            urlBufferIndex = 0;
                        }

                        // advance to the next segment
                        currentSegment = HOSTNAME;
                    }
                } else {
                    // Whoops, this wasn't the scheme. It must have been the hostname.

                    // blank out the scheme
                    offsets[SCHEME][INDEX] = 0;
                    offsets[SCHEME][LENGTH] = 0;

                    // start over, but look for a hostname now, instead of a scheme
                    // (There is a performance tweak that could be implemented here
                    // to avoid starting the scan over, but it's trickier than you might think.)
                    currentSegment = HOSTNAME;
                    offsets[HOSTNAME][INDEX] = NOT_VISITED;
                    offsets[HOSTNAME][LENGTH] = NOT_VISITED;
                    urlBufferIndex = 0;
                    urlIndex = 0;
                }
            }
        }

        // Ran out of url to scan, or no room in the buffer. Keep what we found as the hostname.
        if (urlIndex >= size || urlBufferIndex >= getAssemblyBufferSize()) {
            // check to see if our scheme length has already been calculated
            // if it was, then that means that we found the scheme. Otherwise, take what we got as the hostname.
            if (offsets[SCHEME][LENGTH] == NOT_VISITED) {
                // didn't manage to find the scheme. Just call when we found as the hostname
                offsets[HOSTNAME][INDEX] = offsets[SCHEME][INDEX];
                offsets[HOSTNAME][LENGTH] = urlBufferIndex - offsets[SCHEME][INDEX];

                // set an empty scheme
                offsets[SCHEME][INDEX] = 0;
                offsets[SCHEME][LENGTH] = 0;

                // set an empty path
                offsets[PATH][INDEX] = urlBufferIndex;
                offsets[PATH][LENGTH] = 0;
            }
            keepScanningUrl = false;
        }
    }

    void scanHostname(int size, char c) {
        if (segmentNotFoundYet(HOSTNAME)) {
            if (isAlphanumeric(c)) {
                // We're at the start of this segment, capture the index
                offsets[HOSTNAME][INDEX] = urlBufferIndex;

                // Capture the character into the buffer, and proceed on to the next character.
                urlBuffer[urlBufferIndex++] = toLower(c);
                urlIndex++;
            } else if (c == '/') {
                // It could be that there is no host specified, as in "file:///document.txt"
                offsets[HOSTNAME][INDEX] = urlBufferIndex;
                offsets[HOSTNAME][LENGTH] = 0;
                currentSegment = PATH;
            } else if (c == '.') {
                // trim off leading '.' characters
                urlIndex++;
            } else {
                // not a valid hostname
                keepScanningUrl = false;
            }
        } else {
            // underscores aren't usually permitted in URLs, but we're letting them be here for the chukwa demux.
            if (isAlphanumeric(c) || c == '.' || c == '-' || c == '_') {
                // Capture the character into the buffer, and proceed on to the next character.
                urlBuffer[urlBufferIndex++] = toLower(c);
                urlIndex++;
            } else {
                // found the end of the hostname
                offsets[HOSTNAME][LENGTH] = urlBufferIndex - offsets[HOSTNAME][INDEX];

                // strip off any trailing '.' characters
                while (offsets[HOSTNAME][LENGTH] > 0 && urlBuffer[offsets[HOSTNAME][INDEX] + offsets[HOSTNAME][LENGTH] - 1] == '.') {
                    urlBufferIndex--;
                    offsets[HOSTNAME][LENGTH]--;
                }

                // advance to the next segment
                currentSegment = PORT;
            }
        }

        // Ran out of url to scan, or no room in the buffer. Keep what we found as the host.
        if (urlIndex >= size || urlBufferIndex >= getAssemblyBufferSize()) {
            // set the length of the hostname segment
            offsets[HOSTNAME][LENGTH] = urlBufferIndex - offsets[HOSTNAME][INDEX];

            // strip off any trailing '.' characters
            while (offsets[HOSTNAME][LENGTH] > 0 && urlBuffer[offsets[HOSTNAME][INDEX] + offsets[HOSTNAME][LENGTH] - 1] == '.') {
                urlBufferIndex--;
                offsets[HOSTNAME][LENGTH]--;
            }

            // set an empty path
            offsets[PATH][INDEX] = urlBufferIndex;
            offsets[PATH][LENGTH] = 0;

            keepScanningUrl = false;
        }
    }

    void scanPort(int size, char c) {
        // are we just starting the port segment?
        if (segmentNotFoundYet(PORT)) {
            // yes, just starting. This first character must be a ':' if this is really a port.
            if (c == ':') {
                // yes, it looks like a port. Skip the ':' and proceed, but verify the next character is numeric first
                if (urlIndex + 1 < size) {
                    // there is at least one more character. If it's numeric, then proceed.
                    if (isNumeric(originalUrl.charAt(urlIndex + 1))) {
                        urlIndex++;
                        // don't increment the urlBufferIndex, because we're not keeping the ':' character.
                        offsets[PORT][INDEX] = urlBufferIndex;
                    } else {
                        // the next character isn't numeric. Why do we have a ':' followed by non-numeric characters?
                        // Let's assume there is no port, and move on.
                        urlIndex++;
                        currentSegment = PATH;
                    }
                } else {
                    // There is no next character, so ignore any port information, and eject
                    keepScanningUrl = false;
                }
            } else {
                // not a port. Set an empty port, and move to the next segment
                offsets[PORT][INDEX] = urlBufferIndex;
                offsets[PORT][LENGTH] = 0;
                currentSegment = PATH;
            }
        } else {
            // We're not at the beginning of the port segment. We are either chomping numbers, or have
            // just skipped the leading ':' character.
            if (isNumeric(c)) {
                // add this character to our port, then proceed to the next character.
                urlBuffer[urlBufferIndex++] = c;
                urlIndex++;
            } else {
                // the end of the port has been reached. Store the length, and move to the next segment.
                offsets[PORT][LENGTH] = urlBufferIndex - offsets[PORT][INDEX];
                currentSegment = PATH;
            }
        }

        // If we're past the end, just keep whatever port information we have.
        if (urlIndex >= size || urlBufferIndex >= getAssemblyBufferSize()) {
            // set the length of the port
            offsets[PORT][LENGTH] = urlBufferIndex - offsets[PORT][INDEX];

            // set an empty path
            offsets[PATH][INDEX] = urlBufferIndex;
            offsets[PATH][LENGTH] = 0;

            keepScanningUrl = false;
        }
    }

    void scanPath(int size, char c) {
        // are we just starting with the path segment?
        if (segmentNotFoundYet(PATH)) {
            // just starting. Check the first character
            if (c == '/') {
                // Yep, this is the path. Skip the leading '/' character, we'll add it back when assembling.
                offsets[PATH][INDEX] = urlBufferIndex;
                urlIndex++;
            } else if ((c == '?') || (c == '&') || (c == '#')) {
                // looks like a subsequent segment has started.

                // Set an empty path and move on
                offsets[PATH][INDEX] = urlBufferIndex;
                offsets[PATH][LENGTH] = 0;

                currentSegment = QUERY;
            } else {
                // an unexpected character was encountered. Let's just bail.
                keepScanningUrl = false;
            }
        } else {
            // chomping through the path. Check the current character to see if we're done yet.
            if ((c == '?') || (c == '&') || (c == '#')) {
                // we've found what appears to be the start a later segment, the query.
                // Capture the length, and move to the next segment.
                offsets[PATH][LENGTH] = urlBufferIndex - offsets[PATH][INDEX];
                currentSegment = QUERY;
            } else {
                // check the current character to see if it's an escape sequence
                if (c == '%') {
                    // it's an escape sequence, or at least part of one. Check the next two characters too.
                    if (urlIndex + 2 >= size) {
                        // not enough characters for this to be an escape sequence
                        // just end it here, not including this escape sequence.
                        // set the length of the path segment
                        offsets[PATH][LENGTH] = urlBufferIndex - offsets[PATH][INDEX];
                        keepScanningUrl = false;
                    } else {
                        // check to see if the next two characters are alphanumeric.
                        if (isAlphanumeric(originalUrl.charAt(urlIndex + 1)) && isAlphanumeric(originalUrl.charAt(urlIndex + 2))) {
                            // they are. Looks like a valid escape sequence. Keep it, but uppercase the alphabetic characters.
                            urlBuffer[urlBufferIndex++] = c; // save the '%' here
                            urlIndex++;

                            // preserve the next two characters, but in uppercase
                            urlBuffer[urlBufferIndex++] = toUpper(originalUrl.charAt(urlIndex++));
                            urlBuffer[urlBufferIndex++] = toUpper(originalUrl.charAt(urlIndex++));
                        } else {
                            // they are not both alphanumeric. This is a strange state... just keep the '%'
                            // and move on in case the next characters moves to a different segment or something.
                            urlBuffer[urlBufferIndex++] = c; // save the '%' here
                            urlIndex++;
                        }
                    }
                } else {
                    // check to see if we've got multiple consecutive slashes. We only need one.
                    char nextCharacter = urlIndex + 1 < size ? originalUrl.charAt(urlIndex + 1) : '\0';
                    if (c == '/' && nextCharacter == '/') {
                        // skip this extra slash
                        urlIndex++;
                    } else {
                        // keep chomping
                        urlBuffer[urlBufferIndex++] = c;
                        urlIndex++;
                    }
                }
            }
        }

        // If we're past the end, just keep whatever path information we have.
        if (urlIndex >= size || urlBufferIndex >= getAssemblyBufferSize()) {
            // set the length of the path segment
            offsets[PATH][LENGTH] = urlBufferIndex - offsets[PATH][INDEX];

            keepScanningUrl = false;
        }
    }

    void scanQuery(int size, char c) throws Exception {
        // Check to see if we're just entering this segment.
        if (segmentNotFoundYet(QUERY)) {
            // Yes, we're just entering this segment.
            if (c == '?' || c == '&') {

                // snag the starting point
                offsets[QUERY][INDEX] = urlBufferIndex;

                // skip the '?' or '&' character
                urlIndex++;
            } else if (c == '#') {
                // looks like there is no query, but there is an anchor.
                offsets[QUERY][INDEX] = urlBufferIndex;
                offsets[QUERY][LENGTH] = 0;

                currentSegment = ANCHOR;
            } else {
                // Huh? We shouldn't be able to get here.
                throw new Exception("Entered the QUERY segment, but it started with '" + c + "' instead of '?' or '#'.");
            }
        } else {
            if (c == '#') {
                // We've reached the end of the query segment, and entered the anchor segment.
                offsets[QUERY][LENGTH] = urlBufferIndex - offsets[QUERY][INDEX];
                currentSegment = ANCHOR;
            } else {
                // check the current character to see if it's an escape sequence
                if (c == '%') {
                    // it's an escape sequence, or at least part of one. Check the next two characters too.
                    if (urlIndex + 2 >= size) {
                        // not enough characters for this to be an escape sequence
                        // just end it here, not including this escape sequence.
                        // set the length of the path segment
                        offsets[QUERY][LENGTH] = urlBufferIndex - offsets[QUERY][INDEX];
                        keepScanningUrl = false;
                    } else {
                        // check to see if the next two characters are alphanumeric.
                        if (isAlphanumeric(originalUrl.charAt(urlIndex + 1)) && isAlphanumeric(originalUrl.charAt(urlIndex + 2))) {
                            // they are. Looks like a valid escape sequence. Keep it, but uppercase the alphabetic characters.
                            urlBuffer[urlBufferIndex++] = c; // save the '%' here
                            urlIndex++;

                            // preserve the next two characters, but in uppercase
                            urlBuffer[urlBufferIndex++] = toUpper(originalUrl.charAt(urlIndex++));
                            urlBuffer[urlBufferIndex++] = toUpper(originalUrl.charAt(urlIndex++));
                        } else {
                            // they are not both alphanumeric. This is a strange state... just keep the '%'
                            // and move on in case the next characters moves to a different segment or something.
                            urlBuffer[urlBufferIndex++] = c; // save the '%' here
                            urlIndex++;
                        }
                    }
                } else {
                    // continue to chomp through it
                    urlBuffer[urlBufferIndex++] = c;
                    urlIndex++;
                }
            }
        }

        // If we're past the end, just keep whatever query information we have.
        if (urlIndex >= size || urlBufferIndex >= getAssemblyBufferSize()) {
            offsets[QUERY][LENGTH] = urlBufferIndex - offsets[QUERY][INDEX];
            keepScanningUrl = false;
        }
    }

    void scanAnchor(int size, char c) throws Exception {
        // Check to see if we're just entering this segment.
        if (segmentNotFoundYet(ANCHOR)) {
            // Yes, we're just entering this segment. The first character must be '#'.
            if (c == '#') {
                // snag the starting point
                offsets[ANCHOR][INDEX] = urlBufferIndex;

                // skip the '#' character
                urlIndex++;
            } else {
                // Huh? We shouldn't be able to get here.
                throw new Exception("Entered the ANCHOR segment, but it started with '" + c + "' instead of '#'");
            }
        } else {
            // check the current character to see if it's an escape sequence
            if (c == '%') {
                // it's an escape sequence, or at least part of one. Check the next two characters too.
                if (urlIndex + 2 >= size) {
                    // not enough characters for this to be an escape sequence
                    // just end it here, not including this escape sequence.
                    // set the length of the path segment
                    offsets[ANCHOR][LENGTH] = urlBufferIndex - offsets[ANCHOR][INDEX];
                    keepScanningUrl = false;
                } else {
                    // check to see if the next two characters are alphanumeric.
                    if (isAlphanumeric(originalUrl.charAt(urlIndex + 1)) && isAlphanumeric(originalUrl.charAt(urlIndex + 2))) {
                        // they are. Looks like a valid escape sequence. Keep it, but uppercase the alphabetic characters.
                        urlBuffer[urlBufferIndex++] = c; // save the '%' here
                        urlIndex++;

                        // preserve the next two characters, but in uppercase
                        urlBuffer[urlBufferIndex++] = toUpper(originalUrl.charAt(urlIndex++));
                        urlBuffer[urlBufferIndex++] = toUpper(originalUrl.charAt(urlIndex++));
                    } else {
                        // they are not both alphanumeric. This is a strange state... just ignore the '%'
                        // and move on in case the next characters moves to a different segment or something.
                        urlIndex++;
                    }
                }
            } else {
                // continue to chomp through it
                urlBuffer[urlBufferIndex++] = c;
                urlIndex++;
            }
        }

        // If we're past the end, just keep whatever remaining anchor information we have.
        if (urlIndex >= size || urlBufferIndex >= getAssemblyBufferSize()) {
            offsets[ANCHOR][LENGTH] = urlBufferIndex - offsets[ANCHOR][INDEX];
            keepScanningUrl = false;
        }
    }

    /**
     * Remove all whitespace (spaces, tabs, and newlines) from the string
     * <p/>
     * This uses the assembly buffer, so it will stomp on anything already in it
     *
     * @param url cannot be null
     * @return a new string, but with no whitespace in it
     */
    String removeWhitespace(String url) {
        // this is the index into the assembly buffer
        int assemblyBufferIndex = 0;

        // go through the string, and construct one without any whitespace
        for (int i = 0; i < url.length() && assemblyBufferIndex < getAssemblyBufferSize(); i++) {
            char c = url.charAt(i);
            switch (c) {
                case ' ':
                case '\t':
                case '\n':
                case '\r':
                    break;
                default:
                    assemblyBuffer[assemblyBufferIndex++] = c;
            }
        }
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Checks the URL against some rules that determine if it should be tossed.
     * The URL needs to have been scanned first.
     *
     * @return true if it should be discarded.
     */
    public boolean urlShouldBeDiscarded() {
        return urlShouldBeDiscarded(true, true, true, true, true);
    }

    /**
     * Checks the URL against some rules that determine if it should be tossed.
     * The URL needs to have been scanned first.
     *
     * @param considerEmpty               include a check to see if the url is empty
     * @param considerDashOnly            include a check to see if the url is just a single '-' character
     * @param considerScheme              include a check against the scheme. For example: file:///index.html
     * @param considerHostname            include a check of some basic hostname logic; length, a dot, etc.
     * @param considerHostnameAsIpAddress include a check of the hostname to see if it only includes numbers and dots
     * @return true if it should be discarded.
     */
    public boolean urlShouldBeDiscarded(
            boolean considerEmpty,
            boolean considerDashOnly,
            boolean considerScheme,
            boolean considerHostname,
            boolean considerHostnameAsIpAddress) {
        return (considerEmpty && (originalUrl == null || originalUrl.length() == 0))
                || (considerDashOnly && (originalUrl.charAt(0) == '-'))
                || (considerScheme && (isSegmentEqualToString(SCHEME, "file")))
                || (considerHostname && !hostnameSeemsValid())
                || (considerHostnameAsIpAddress && hostnameIsIpAddress());
    }

    /**
     * Do some basic sanity checks on the hostname.
     * The URL needs to have been scanned first.
     *
     * @return true if it seems valid.
     */
    boolean hostnameSeemsValid() {

        // if we don't have one, it certainly isn't valid.
        if (offsets[HOSTNAME][INDEX] == NOT_VISITED || offsets[HOSTNAME][LENGTH] == NOT_VISITED) {
            return false;
        }

        // the shortest possible valid real hostname is 4 characters. For example: "a.ly" is valid.
        if (offsets[HOSTNAME][LENGTH] < 4) {
            return false;
        }

        // hasn't seen a '.' character yet.
        boolean foundDot = false;

        // check for at least one '.' character.
        for (int i = 0; i < offsets[HOSTNAME][LENGTH]; i++) {
            char c = urlBuffer[offsets[HOSTNAME][INDEX] + i];
            if (c == '.') {
                foundDot = true;
            }
        }
        return foundDot;
    }

    /**
     * Returns true if it only contains numbers are '.' characters.
     * The URL needs to have been scanned first.
     *
     * @return true if it looks like it's probably an IP address.
     */
    boolean hostnameIsIpAddress() {
        // check if it's too short to be a valid IP address
        // 1.2.3.4
        if (offsets[HOSTNAME][LENGTH] < 7) {
            return false;
        }

        for (int i = 0; i < offsets[HOSTNAME][LENGTH]; i++) {
            char c = urlBuffer[offsets[HOSTNAME][INDEX] + i];
            if (!isNumeric(c) && c != '.') {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks a segment to see if it has been found and actually has any data in it.
     *
     * @param segmentIdentifier one of QUERY, HOSTNAME, etc
     * @return true if there is data for the specified segment.
     */
    boolean segmentDataExists(int segmentIdentifier) {
        return offsets[segmentIdentifier][INDEX] != NOT_VISITED && offsets[segmentIdentifier][LENGTH] > 0;
    }

    /**
     * blank out the set of positions and lengths
     */
    void resetAllMarkers() {
        for (int i = 0; i < MAX_POSITIONS_NOTED; i++) {
            for (int j = 0; j < MAX_COLUMNS; j++) {
                offsets[i][j] = NOT_VISITED;
            }
        }
    }

    int getUrlBufferSize() {
        return urlBufferSize;
    }

    int getAssemblyBufferSize() {
        return assemblyBufferSize;
    }

    /**
     * Emits URLs for the purpose of grouping data on them. It favors a smaller URL, strips off all schemes,
     * ports, and anchors, as well as leading "www." and trailing "index.html" sorts of things.
     *
     * @return the assembled URL
     */
    public String getUrlNormalizedForGrouping() {
        assemblyBufferIndex = 0;
        assembleHostname(false);
        assemblePath(false, true);
        assembleQuery();
        return performBuiltInNormalization(new String(assemblyBuffer, 0, assemblyBufferIndex));
    }

    /**
     * Emits URLs as close as possible to what came in, but normalized.
     *
     * @return the url
     */
    public String getUrlNormalizedAsRawAsPossible() {
        assemblyBufferIndex = 0;
        assembleScheme();
        assembleHostname(true);
        assemblePortIfNot80();
        assemblePath(false, false);
        assembleQuery();
        assembleAnchor();
        return performBuiltInNormalization(new String(assemblyBuffer, 0, assemblyBufferIndex));
    }

    /**
     * Emits URLs for the purpose of grouping data on them. It favors a smaller URL, strips off all schemes,
     * ports, and anchors, as well as leading "www." and trailing "index.html" sorts of things.
     *
     * @return the assembled URL
     */
    public String getUrlNormalizedForGroupingReversingHostname() {

        // get the reversed hostname first. This will munge the assembly buffer, but that's okay.
        String reversedHostname = getSegmentHostnameReversed();

        // now overwrite the contents of the assembly buffer
        assemblyBufferIndex = 0;

        // first, put in the reversed hostname
        insertStringIntoAssemblyBuffer(reversedHostname);
        assemblePath(false, true);
        assembleQuery();

        return performBuiltInNormalization(new String(assemblyBuffer, 0, assemblyBufferIndex));
    }

    /**
     * Emits URLs for the purpose of grouping data on them, but ignoring the document name and everything following.
     *
     * @return the assembled URL
     */
    public String getUrlNormalizedForGroupingByPath() {
        assemblyBufferIndex = 0;
        assembleHostname(false);
        assemblePath(true, true);
        return performBuiltInNormalization(new String(assemblyBuffer, 0, assemblyBufferIndex));
    }

    /**
     * Emits URLs for the purpose of generating a usable URL that can be used as a live link. It adds
     * an "http://" scheme if there was no scheme present, and strips off ports only if they aren't "80". It removes
     * anchors.
     *
     * @return the assembled URL
     */
    public String getUrlNormalizedAndLinkable() {
        assemblyBufferIndex = 0;
        assembleScheme();
        assembleHostname(true);
        assemblePortIfNot80();
        assemblePath(false, false);
        assembleQuery();
        return performBuiltInNormalization(new String(assemblyBuffer, 0, assemblyBufferIndex));
    }

    /**
     * Obtain this segment from the urlBuffer and copy it into the urlBuffer. If there is no scheme, create "http://".
     */
    void assembleScheme() {
        // put a scheme on it
        if (!segmentDataExists(SCHEME)) {
            // we don't have a scheme. Just use "http" here.
            insertStringIntoAssemblyBuffer("http");
        } else {
            // get the scheme
            for (int i = offsets[SCHEME][INDEX]; i < (offsets[SCHEME][INDEX] + offsets[SCHEME][LENGTH]); i++) {
                assemblyBuffer[assemblyBufferIndex++] = urlBuffer[i];
            }
        }

        // apply the "://" glue.
        insertStringIntoAssemblyBuffer("://");
    }

    /**
     * Obtain this segment from the urlBuffer and copy it into the urlBuffer
     *
     * @param keepPrecedingWww set this to TRUE to retain a "www.", or false to chop it off.
     *                         This also includes any characters between "www" and the first '.' in the hostname.
     *                         (a part of a hostname is what is between the dots.)
     *                         if there are 3 or more parts, and the first part starts with "www", drop the first part.
     */
    void assembleHostname(boolean keepPrecedingWww) {

        // get the hostname
        if (segmentDataExists(HOSTNAME)) {

            // number of characters to skip at the start of the hostname.
            int charactersToSkip = 0;

            // if we are throwing out "www", then throw out the "www".
            if (!keepPrecedingWww) {
                // this steps on i and sets k to the number of characters to skip at the start of the hostname
                charactersToSkip = omitPrecedingWww();
            }

            // gather the desired characters from the hostname, skipping the first k characters.
            for (int i = offsets[HOSTNAME][INDEX] + charactersToSkip; i < (offsets[HOSTNAME][INDEX] + offsets[HOSTNAME][LENGTH]); i++) {
                assemblyBuffer[assemblyBufferIndex++] = urlBuffer[i];
            }
        }
    }

    /**
     * Obtain this segment from the urlBuffer and copy it into the urlBuffer. If the port is 80, it is ignored.
     */
    void assemblePortIfNot80() {
        // include the port only if it's not "80".
        if (segmentDataExists(PORT)) {
            // check to see if the port is 80
            //noinspection StatementWithEmptyBody
            if ((offsets[PORT][LENGTH] == 2)
                    && (urlBuffer[offsets[PORT][INDEX]] == '8')
                    && (urlBuffer[offsets[PORT][INDEX] + 1] == '0')) {
                // the port is 80, don't bother to include it
            } else {
                // the port isn't 80, so include it in this linkable URL
                assemblyBuffer[assemblyBufferIndex++] = ':';
                for (int i = offsets[PORT][INDEX]; i < (offsets[PORT][INDEX] + offsets[PORT][LENGTH]); i++) {
                    assemblyBuffer[assemblyBufferIndex++] = urlBuffer[i];
                }
            }
        }
    }

    /**
     * Obtain this segment from the urlBuffer and copy it into the urlBuffer.
     *
     * @param alwaysDropDocument  should be FALSE if you want the full path, or TRUE to omit the document part.
     *                            Since it is impossible to determine whether the text following the last '/' in the path is a document or
     *                            another path (the web server decides), it is treated as a document.
     * @param dropDefaultDocument should be TRUE if you want to change, for example, "z.com/abc/index.html" into
     *                            "z.com/abc/".
     */
    void assemblePath(boolean alwaysDropDocument,
                              boolean dropDefaultDocument) {

        // here, this flag is TRUE if we should check to see if a '/' should be appended to the end
        boolean possiblyAppendTrailingSlash = false;

        if (segmentDataExists(PATH)) {

            // charactersToSkip is the original length of the path segment
            int charactersToSkip = offsets[PATH][LENGTH];

            if (alwaysDropDocument) {
                // start with the length, and scan backwards looking for the last '/' character in the path.
                for (charactersToSkip = offsets[PATH][LENGTH]; charactersToSkip > 0; charactersToSkip--) {
                    if (urlBuffer[offsets[PATH][INDEX] + charactersToSkip - 1] == '/') {
                        break;
                    }
                }
                // k now is now the new length of the path segment, but lacking the document part at the end.
            } else if (dropDefaultDocument) {

                // check for "index.html"
                if (offsets[PATH][LENGTH] >= 10
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 1] == 'l'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 2] == 'm'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 3] == 't'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 4] == 'h'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 5] == '.'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 6] == 'x'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 7] == 'e'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 8] == 'd'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 9] == 'n'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 10] == 'i'
                        // the first '/' isn't stored in the urlBuffer
                        && (offsets[PATH][LENGTH] == 10 || urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 11] == '/')) {
                    // shrink the length, to chop off this last bit of the path, keeping a trailing '/' character, if it's there.
                    charactersToSkip -= 10;

                    // if this default document was the only thing here, chop off the slash, and indicate that we need
                    // to check later to see if we need to add it back on.
                    if (offsets[PATH][LENGTH] == 10) {
                        possiblyAppendTrailingSlash = true;
                        charactersToSkip--;
                    }
                }

                // check for "default.asp"
                else if (offsets[PATH][LENGTH] >= 11
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 1] == 'p'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 2] == 's'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 3] == 'a'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 4] == '.'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 5] == 't'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 6] == 'l'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 7] == 'u'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 8] == 'a'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 9] == 'f'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 10] == 'e'
                        && urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 11] == 'd'
                        // the first '/' isn't stored in the urlBuffer
                        && (offsets[PATH][LENGTH] == 11 || urlBuffer[offsets[PATH][INDEX] + offsets[PATH][LENGTH] - 12] == '/')) {
                    // shrink the length, to chop off this last bit of the path, keeping a trailing '/' character, if it's there.
                    charactersToSkip -= 11;

                    // if this default document was the only thing here, chop off the slash, and indicate that we need
                    // to check later to see if we need to add it back on.
                    if (offsets[PATH][LENGTH] == 11) {
                        possiblyAppendTrailingSlash = true;
                        charactersToSkip--;
                    }
                }
            }

            // a path exists, and we may have chopped off some document
            // remember that k here is the length of the path that we will use for now

            // If there is some path information to include, we need to prefix it with a '/' character.
            if (charactersToSkip > 0) {
                assemblyBuffer[assemblyBufferIndex++] = '/';
            }

            // now go through and get the path, and put it into the assembly buffer
            for (int i = offsets[PATH][INDEX]; i < (offsets[PATH][INDEX] + charactersToSkip); i++) {
                assemblyBuffer[assemblyBufferIndex++] = urlBuffer[i];
            }
        }
        // there is no PATH segment defined
        else {
            // later we must check to see if we need to add a '/' character
            possiblyAppendTrailingSlash = true;
        }

        // now check to see if we need to put the trailing '/' character on the path
        //noinspection PointlessBooleanExpression,ConstantConditions
        if (possiblyAppendTrailingSlash && segmentDataExists(HOSTNAME) && !segmentDataExists(QUERY)) {
            assemblyBuffer[assemblyBufferIndex++] = '/';
        }
    }

    /**
     * Obtain this segment from the urlBuffer and copy it into the urlBuffer.
     */
    void assembleQuery() {
        // get the query, if there is one and we want it
        if (segmentDataExists(QUERY)) {
            continueAssemblingQuery();
        }
    }

    /**
     * This cleans up the query parameter identification characters, and puts the string into the assembly buffer
     */
    void continueAssemblingQuery() {

        // start here
        int urlBufferIndex = offsets[QUERY][INDEX];

        // trim off initial unneeded parameter characters
        while (urlBufferIndex < offsets[QUERY][INDEX] + offsets[QUERY][LENGTH] &&
                (urlBuffer[urlBufferIndex] == '?' || urlBuffer[urlBufferIndex] == '&')) {
            // skip over this repeated character
            urlBufferIndex++;
        }

        // if there is any query string left, handle it
        if (urlBufferIndex < offsets[QUERY][INDEX] + offsets[QUERY][LENGTH]) {

            // the query starts with '?', so add it
            assemblyBuffer[assemblyBufferIndex++] = '?';

            // now add the query string, with cleaned up parameters
            for (; urlBufferIndex < (offsets[QUERY][INDEX] + offsets[QUERY][LENGTH]); urlBufferIndex++) {
                char c = urlBuffer[urlBufferIndex];
                if (c == '?' || c == '&') {
                    // clean up the parameter indicators

                    while (urlBufferIndex + 1 < offsets[QUERY][INDEX] + offsets[QUERY][LENGTH] &&
                            (urlBuffer[urlBufferIndex + 1] == '?' || urlBuffer[urlBufferIndex + 1] == '&')) {
                        // skip over this repeated character
                        urlBufferIndex++;
                    }

                    // now just put in the proper character
                    // Note that we can't just put in a '&' character here, because sometimes a parameter contains
                    // another URL that has it's own parameters in them. We don't want to replace the inital '?' with
                    // an incorrect '&' character in that case.
                    // (Also, don't put a character here if there isn't another query.)
                    if (urlBufferIndex < offsets[QUERY][INDEX] + offsets[QUERY][LENGTH] - 1) {
                        assemblyBuffer[assemblyBufferIndex++] = c;
                    }
                } else {
                    assemblyBuffer[assemblyBufferIndex++] = urlBuffer[urlBufferIndex];
                }
            }
        }
    }

    /**
     * Obtain this segment from the urlBuffer and copy it into the urlBuffer.
     */
    void assembleAnchor() {

        // get the query, if there is one and we want it
        if (segmentDataExists(ANCHOR)) {

            // the query starts with '?', so add it
            assemblyBuffer[assemblyBufferIndex++] = '#';

            // now add the anchor
            for (int i = offsets[ANCHOR][INDEX]; i < (offsets[ANCHOR][INDEX] + offsets[ANCHOR][LENGTH]); i++) {
                assemblyBuffer[assemblyBufferIndex++] = urlBuffer[i];
            }
        }
    }

    /**
     * Obtain this segment of the previously-scanned URL.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentScheme() {
        assemblyBufferIndex = 0; //
        if (segmentDataExists(HOSTNAME) || segmentDataExists(PATH)) {
            assembleScheme();
        }
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Obtain this segment of the previously-scanned URL.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentHostname() {
        assemblyBufferIndex = 0;
        assembleHostname(true);
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Obtain this segment of the previously-scanned URL, but in reverse order (i.e. com.cj)
     * It will not include a preceeding "www."
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentHostnameReversed() {
        assemblyBufferIndex = 0;
        assembleHostname(false);
        int indexOfStartOfReversedHostname = reverseHostnameInAssemblyBuffer();
        return new String(assemblyBuffer, indexOfStartOfReversedHostname, getAssemblyBufferSize() - indexOfStartOfReversedHostname);
    }

    /**
     * Obtain this segment of the previously-scanned URL
     * (Note that this will throw out valid parts of URLs that have three or more pieces, and the first one
     * starts with "www", such as "http://wwwlink.co.uk" will return "co.uk" as the hostname.)
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentHostnameWithoutPrecedingWww() {
        assemblyBufferIndex = 0;
        assembleHostname(false);
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Obtain this segment of the previously-scanned URL. It will return an empty string if the port was not there,
     * or if it was 80.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentPort() {
        assemblyBufferIndex = 0;
        assemblePortIfNot80();
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Obtain this segment of the previously-scanned URL.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentPath() {
        assemblyBufferIndex = 0;
        assemblePath(false, true);
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Obtain this segment of the previously-scanned URL.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentPathWithoutDocument() {
        assemblyBufferIndex = 0;
        assemblePath(true, true);
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Obtain this segment of the previously-scanned URL. A document is the last part of the path, only everything
     * after the last slash. Note that sometimes this tool cannot distinguish between a directory and a document,
     * so "z.com/abc/def" will return "def" as the document, even if it's a directory. Use with caution.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentDocument() {
        assemblyBufferIndex = 0;

        // this will load the assembly buffer up with the path
        assemblePath(false, false);

        // this value will be set by the next for loop.
        int charactersToSkip;

        // scan through the path looking for the last '/' character.
        for (charactersToSkip = assemblyBufferIndex; charactersToSkip > 0; charactersToSkip--) {
            if (assemblyBuffer[charactersToSkip - 1] == '/') {
                break;
            }
        }

        // k now is now the new length of the path segment, but lacking the document part at the end.
        // return a string comprised of only the document portion.
        return new String(assemblyBuffer, charactersToSkip, assemblyBufferIndex - charactersToSkip);
    }

    /**
     * Obtain this segment of the previously-scanned URL.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentQuery() {
        assemblyBufferIndex = 0;
        assembleQuery();
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * Obtain this segment of the previously-scanned URL.
     *
     * @return the segment, or an empty string if it doesn't exist.
     */
    public String getSegmentAnchor() {
        assemblyBufferIndex = 0;
        assembleAnchor();
        return new String(assemblyBuffer, 0, assemblyBufferIndex);
    }

    /**
     * This skips "www45454." and "www." kinds of things.
     *
     * @return how many character to skip
     */
    int omitPrecedingWww() {

        int dotsSeen = 0;
        int charactersToSkip = 0;

        // count the parts of the hostname
        // charactersToSkip is the number of '.' characters seen in the hostname.
        for (int i = offsets[HOSTNAME][INDEX]; i < (offsets[HOSTNAME][INDEX] + offsets[HOSTNAME][LENGTH]); i++) {
            dotsSeen += urlBuffer[i] == '.' ? 1 : 0;
        }

        // if there are 3 or more parts, see about trimming an initial "www." segment
        // (note that dotsSeen is the number of dots, so number of parts = dotsSeen + 1)
        if (dotsSeen >= 2) {
            // determine the length of the first part
            int lengthOfFirstPartOfHostname = 0;
            for (int i = offsets[HOSTNAME][INDEX]; urlBuffer[i] != '.'; i++) {
                lengthOfFirstPartOfHostname++;
            }

            // check to see if the first part starts with "www"
            if (lengthOfFirstPartOfHostname >= 3
                    && urlBuffer[offsets[HOSTNAME][INDEX]] == 'w'
                    && urlBuffer[offsets[HOSTNAME][INDEX] + 1] == 'w'
                    && urlBuffer[offsets[HOSTNAME][INDEX] + 2] == 'w') {
                // it does, so we want to skip the entire first part of the URL.

                // include the trailing '.' character (to be skipped with the rest of the first part).
                charactersToSkip = lengthOfFirstPartOfHostname + 1;
            } else {
                // don't skip anything
                charactersToSkip = 0;
            }
        } else {
            // don't skip anything
            charactersToSkip = 0;
        }
        return charactersToSkip;
    }

    /**
     * Copies the string, character by characrer, into the assembly buffer. Note that l will be stomped on,
     * and j must be the current index into the buffer.
     *
     * @param string to be assembled
     */
    void insertStringIntoAssemblyBuffer(String string) {
        for (int i = 0; i < string.length(); i++) {
            assemblyBuffer[assemblyBufferIndex++] = string.charAt(i);
        }
    }

    /**
     * This method compares a segment in the buffer with the specified string, while avoiding allocating memory
     * for new String objects.
     *
     * @param segmentIdentifier the index into the array of positions, HOSTNAME, PATH, etc
     * @param string            the specified string to compare with.
     * @return true if they are the same, false otherwise
     */
    boolean isSegmentEqualToString(int segmentIdentifier, String string) {
        // if the string is null, the index must be invalid in order to be equal
        if (string == null) {
            return offsets[segmentIdentifier][INDEX] == NOT_VISITED;
        }

        // if the string is empty, and the index is invalid, they are equal.
        if (offsets[segmentIdentifier][INDEX] == NOT_VISITED) {
            return string.length() == 0;
        }

        // they can't be equals if they are of different lengths
        if (string.length() != offsets[segmentIdentifier][LENGTH]) {
            return false;
        }

        // walk through them, and compare side by side
        assemblyBufferIndex = offsets[segmentIdentifier][INDEX];
        for (int i = 0; i < offsets[segmentIdentifier][LENGTH]; i++) {
            if (string.charAt(i) != urlBuffer[assemblyBufferIndex++]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Find the starting index in the urlBuffer of the specified token, if it exists.
     *
     * @param token         the string to look for
     * @param segmentIndex  the starting point in the urlBuffer to start looking
     * @param segmentLength the length of the segment to look inside of. No inspection will be done outside of it.
     * @return the position of the token relative to the beginning of the segment, or -1 if it was not found.
     */
    int findIndexOfToken(String token, int segmentIndex, int segmentLength) {

        // length of the token is pre-calculated for performance
        int tokenLength = token.length();

        // check to see if it even fits
        if (tokenLength > segmentLength) {
            // won't fit, so it isn't present
            return -1;
        }

        // potentialTokenStart is the potential starting position of the token in the segment.
        for (int potentialTokenStart = segmentIndex; potentialTokenStart <= (segmentIndex + segmentLength - tokenLength); potentialTokenStart++) {

            // i is the index in the currently-being-inspected portion of the segment
            // step through the token, and compare each character with the one in the urlBuffer,
            // incrementing potentialTokenStart only if it matches
            int i;
            //noinspection StatementWithEmptyBody
            for (i = 0; (i < tokenLength) && (token.charAt(i) == urlBuffer[potentialTokenStart + i]); i++) ;

            // if potentialTokenStart is the same size as the token, that means that all the characters in the token matched the buffer,
            // and we've found the token.
            if (i == tokenLength) {
                return potentialTokenStart;
            }
        }

        // did not find the token
        return -1;
    }

    /**
     * Helper method to check on the valid status of a segment.
     *
     * @param segmentIdentifier QUERY, HOST, whatever.
     * @return true if it thinks it is valid.
     */
    boolean segmentNotFoundYet(int segmentIdentifier) {
        return (offsets[segmentIdentifier][INDEX] == NOT_VISITED);
    }

    /**
     * Removing dot-segments. The segments “..” and “.” are usually removed from a URL according
     * to the algorithm described in RFC 3986 (or a similar algorithm).
     * This step also converts "http://" into "", and "z.com/ab////cd" into "z.com/ab/cd"
     *
     * @param url the URL to touch
     * @return the cleaned URL
     */
    String performBuiltInNormalization(String url) {
        try {
            // All "." segments are removed.
            // If a ".." segment is preceded by a non-".." segment then both of these segments are removed.
            // Plus some other stuff. See the documentation for scanUrl()
            return new URI(url).normalize().toString();
        } catch (URISyntaxException e) {
            return "";
        }
    }

    /**
     * Using the assembly buffer, reverse the order of the dot-separated segments.
     * This happens in-place, inside the assembly buffer. It sticks the reversed version at the end.
     *
     * @return the index of the start of the reversed hostname in the assembly buffer
     */
    @SuppressWarnings({"ManualArrayCopy"})
    int reverseHostnameInAssemblyBuffer() {
        int lengthOfHostname = assemblyBufferIndex;

        // shift the buffer to the left if the hostname is larger than the assembly buffer
        final int maxAllowedHostnameLength = getAssemblyBufferSize() >> 1;
        if (lengthOfHostname > maxAllowedHostnameLength) {
            int shiftAmount = lengthOfHostname - maxAllowedHostnameLength;
            for (int i = 0; i < maxAllowedHostnameLength; i++) {
                assemblyBuffer[i] = assemblyBuffer[i + shiftAmount];
            }
            lengthOfHostname = maxAllowedHostnameLength;
        }

        int indexInBack = getAssemblyBufferSize();
        int indexWordStart = 0;
        int indexWordEnd = 0;
        while (indexWordEnd <= lengthOfHostname) {

            // we are at the end if it's off the end of the hostname in the assembly buffer,
            // or if we're past the halfway point in the buffer.
            boolean atTheEnd = indexWordEnd == lengthOfHostname;

            // check to see if we're at the end of a word
            if (atTheEnd || assemblyBuffer[indexWordEnd] == '.') {

                // copy word to end of buffer
                indexInBack = indexInBack - (indexWordEnd - indexWordStart);
                for (int i = indexWordStart; i < indexWordEnd; i++) {
                    assemblyBuffer[indexInBack + i - indexWordStart] = assemblyBuffer[i];
                }

                // break out of the loop now
                if (atTheEnd) {
                    break;
                }

                // include the dot
                assemblyBuffer[--indexInBack] = '.';

                // go on to the next word (+1 to skip the dot)
                indexWordStart = indexWordEnd + 1;
                indexWordEnd = indexWordStart;
            } else {
                indexWordEnd++;
            }
        }
        return indexInBack;
    }

    static boolean isAlphanumeric(char c) {
        return isAlphabetic(c) || isNumeric(c);
    }

    static boolean isAlphabetic(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    static boolean isNumeric(char c) {
        return c >= '0' && c <= '9';
    }

    static char toLower(char c) {
        return ((c >= 'A') && (c <= 'Z')) ? (char) (c + ('a' - 'A')) : c;
    }

    static char toUpper(char c) {
        return ((c >= 'a') && (c <= 'z')) ? (char) (c - ('a' - 'A')) : c;
    }

    public static void main(String... args) {
        if (args.length < 1) {
            System.out.println("Specify command:");
            System.out.println("\tparse INPUT_FILENAME (This creates INPUT_FILENAME.out and INPUT_FILENAME.err)");
            System.out.println("\tjoin INPUT_FILENAME_SYNTRIX_URLS INPUT_FILENAME_URLS_FROM_LOGS (This creates the file 'joined.out')");
            System.out.println("\tunreverse INPUT_FILENAME (This creates the file INPUT_FILENAME.out");
        } else {
            if ("parse".equalsIgnoreCase(args[0])) {
                if (args.length < 2) {
                    System.out.println("Specify the input filename too.");
                } else {
                    String inputFilename = args[1];
                    String outputFilename = inputFilename + ".out";
                    String errorFilename = inputFilename + ".err";
                    FUrl tool = new FUrl();

                    int lineCount = 0;
                    int recordCount = 0;
                    int errorCount = 0;
                    int invalidCount = 0;
                    int validCount = 0;
                    String line;
                    BufferedReader reader = null;
                    PrintWriter writer = null;
                    PrintWriter errorWriter = null;
                    try {
                        reader = new BufferedReader(new FileReader(inputFilename));
                        writer = new PrintWriter(outputFilename);
                        errorWriter = new PrintWriter(errorFilename);
                        while ((line = reader.readLine()) != null) {
                            lineCount++;
                            String[] tabDividedPieces = line.split("\\t");
                            if (tabDividedPieces.length > 1) {
                                String[] commaDividedPieces = tabDividedPieces[1].split(",");
                                if (commaDividedPieces.length < 3) {
                                    errorCount++;
                                    System.out.println("Found bad data (comma) on line " + lineCount);
                                } else {
                                    try {
                                        tool.scanUrl(commaDividedPieces[2]);
                                        if (!tool.hostnameSeemsValid() || tool.hostnameIsIpAddress()) {
                                            invalidCount++;
                                        } else {
                                            writer.print(tool.getSegmentHostnameWithoutPrecedingWww());
                                            writer.print(",");
                                            writer.println(commaDividedPieces[1]);
                                        }
                                    } catch (IOException e) {
                                        errorCount++;
                                        System.out.println("URL parsing error on line " + lineCount);
                                        errorWriter.println(line);
                                    }
                                    recordCount++;
                                    if (recordCount % 100000 == 0) {
                                        System.out.println("Parsed=" + recordCount
                                                + " errors=" + errorCount
                                                + " valid=" + validCount
                                                + " invalid=" + invalidCount);
                                    }
                                }
                            }
                        }
                        System.out.println("Total Parsed=" + recordCount
                                + " errors=" + errorCount
                                + " valid=" + validCount
                                + " invalid=" + invalidCount);
                    } catch (IOException e) {
                        System.out.println("error: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                            if (writer != null) {
                                writer.close();
                            }
                            if (errorWriter != null) {
                                errorWriter.close();
                            }
                        } catch (IOException ee) {
                            System.out.println("Another error: " + ee.getMessage());
                            ee.printStackTrace();
                        }
                    }
                }
            } else if ("join".equalsIgnoreCase(args[0])) {
                if (args.length < 3) {
                    System.out.println("Specify the filenames of the files to be joined too.");
                } else {
                    String filenameSyntryx = args[1];
                    String filenameFromLogs = args[2];
                    String filenameResult = "joined.out";
                    String filenameError = "joined.err";

                    BufferedReader readerSyntryx = null;
                    BufferedReader readerFromLogs = null;
                    PrintWriter writer = null;
                    PrintWriter errorWriter = null;
                    try {
                        readerSyntryx = new BufferedReader(new FileReader(filenameSyntryx));
                        readerFromLogs = new BufferedReader(new FileReader(filenameFromLogs));
                        writer = new PrintWriter(filenameResult);
                        errorWriter = new PrintWriter(filenameError);

                        int lineCount = 0;
                        int loadCount = 0;
                        int skipCount = 0;
                        String line;

                        // load all the syntryx URLs into a hashset to eliminate dupes
                        Set<String> syntryxUrls = new HashSet<String>();
                        FUrl tool = new FUrl();
                        while ((line = readerSyntryx.readLine()) != null) {
                            lineCount++;
                            try {
                                tool.scanUrl(line);
                                if (tool.hostnameIsIpAddress() || !tool.hostnameSeemsValid()) {
                                    skipCount++;
                                } else {
                                    syntryxUrls.add(tool.getSegmentHostnameReversed() + ".");
                                    loadCount++;
                                }
                            } catch (IOException e) {
                                System.out.println("Failed to parse URL from " + filenameSyntryx + " on line " + lineCount);
                                errorWriter.println("line " + lineCount + ": " + line);
                            }
                            if (lineCount % 10000 == 0) {
                                System.out.println("Loaded " + loadCount + " URLs so far. Skipped " + skipCount);
                            }
                        }
                        int numSyntryxUrls = syntryxUrls.size();
                        readerSyntryx.close();
                        readerSyntryx = null;

                        // construct a map of URL length to list of URLs with that length
                        Map<Integer, List<String>> syntryxMapBySize = new HashMap<Integer, List<String>>();
                        for (String syntryxUrl : syntryxUrls) {
                            int length = syntryxUrl.length();
                            List<String> list = syntryxMapBySize.get(length);
                            if (list == null) {
                                list = new ArrayList<String>();
                                syntryxMapBySize.put(length, list);
                            }
                            list.add(syntryxUrl);
                        }

                        // construct an arraylist in URL length order, longest first
                        List<String> syntryxBySize = new ArrayList<String>(numSyntryxUrls);
                        for (int i = 100; i > 0; i--) {
                            List<String> list = syntryxMapBySize.get(i);
                            if (list != null) {
                                syntryxBySize.addAll(list);
                            }
                        }

                        if (syntryxUrls.size() == 0) {
                            System.out.println("No URLs loaded! Exiting.");
                        } else {
                            System.out.println("Loaded " + loadCount + " URLs. After dupe removal, now have " + syntryxUrls.size() + " of them.");
                            int joined = 0;
                            int missed = 0;
                            int hostnameInvalid = 0;
                            int logUrlsInspected = 0;
                            int logUrlsInError = 0;
                            lineCount = 0;
                            String lineFromLog;
                            while ((lineFromLog = readerFromLogs.readLine()) != null) {
                                lineCount++;
                                try {
                                    String[] pieces = lineFromLog.split(",");
                                    if (pieces.length != 2) {
                                        System.out.println("Wrong number of elements in log entry");
                                        logUrlsInError++;
                                    } else {
                                        tool.scanUrl(pieces[0]);
                                        if (tool.hostnameIsIpAddress() || !tool.hostnameSeemsValid()) {
                                            hostnameInvalid++;
                                        } else {
                                            String urlFromLogs = tool.getSegmentHostnameReversed() + ".";
                                            logUrlsInspected++;
                                            boolean foundOne = false;
                                            for (String syntryxUrl : syntryxBySize) {
                                                if ((urlFromLogs.length() >= syntryxUrl.length()) && (urlFromLogs).startsWith(syntryxUrl)) {
                                                    writer.print(removeDot(syntryxUrl));
                                                    writer.print(",");
                                                    writer.print(removeDot(urlFromLogs));
                                                    writer.print(",");
                                                    writer.println(pieces[1]);
                                                    foundOne = true;
                                                    break;
                                                }
                                            }
                                            if (foundOne) {
                                                joined++;
                                            } else {
                                                missed++;
                                            }
                                        }
                                    }
                                } catch (IOException e) {
                                    System.out.println("Failed to parse URL from " + filenameFromLogs + " on line " + lineCount);
                                    logUrlsInError++;
                                }
                                if (lineCount % 1000 == 0) {
                                    System.out.println("Log URLs inspected=" + logUrlsInspected
                                            + " Joined=" + joined
                                            + " Missed=" + missed
                                            + " Invalid=" + hostnameInvalid
                                            + " Errors=" + logUrlsInError
                                            + " so far.");
                                }
                            }
                            System.out.println("Log URLs inspected=" + logUrlsInspected
                                    + " Joined=" + joined
                                    + " Missed=" + missed
                                    + " Invalid=" + hostnameInvalid
                                    + " Errors=" + logUrlsInError
                                    + " total.");
                        }
                    } catch (IOException e) {
                        System.out.println("error: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        try {
                            if (readerSyntryx != null) {
                                readerSyntryx.close();
                            }
                            if (readerFromLogs != null) {
                                readerFromLogs.close();
                            }
                            if (writer != null) {
                                writer.close();
                            }
                            if (errorWriter != null) {
                                errorWriter.close();
                            }
                        } catch (IOException ee) {
                            System.out.println("Another error: " + ee.getMessage());
                            ee.printStackTrace();
                        }
                    }
                }
            } else if ("unreverse".equalsIgnoreCase(args[0])) {
                if (args.length < 2) {
                    System.out.println("Specify the input filename.");
                } else {
                    String filenameIn = args[1];
                    String filenameOut = filenameIn + ".out";
                    BufferedReader reader = null;
                    PrintWriter writer = null;
                    try {
                        reader = new BufferedReader(new FileReader(filenameIn));
                        writer = new PrintWriter(filenameOut);
                        int lineCount = 0;
                        String line;
                        while ((line = reader.readLine()) != null) {
                            lineCount++;
                            String[] pieces = line.split(",");
                            if (pieces.length == 3) {
                                writer.print(unreverseHostname(pieces[0]));
                                writer.print(",");
                                writer.print(unreverseHostname(pieces[1]));
                                writer.print(",");
                                writer.println(pieces[2]);
                            }
                        }
                        System.out.println("Completed. lines processed: " + lineCount);
                    } catch (IOException e) {
                        System.out.println("error: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                            if (writer != null) {
                                writer.close();
                            }
                        } catch (IOException ee) {
                            System.out.println("Another error: " + ee.getMessage());
                            ee.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    static String unreverseHostname(String hostname) {
        String[] split = hostname.split("\\.");
        int len = split.length;
        if (len == 0) {
            return "";
        }
        if (len == 1) {
            return split[0];
        }
        StringBuilder builder = new StringBuilder();
        builder.append(split[len - 1]);
        for (int i = len - 2; i >= 0; i--) {
            builder.append(".");
            builder.append(split[i]);
        }
        return builder.toString();
    }

    static String removeDot(String s) {
        return s.substring(0, s.length() - 1);
    }

}
