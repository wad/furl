package com.zerbat.furl;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class FUrlTest {

    private static final String TRAILING_SLASH = "/";

    @Test
    public void testUriNormalizerEmpty() throws IOException {
        FUrl FUrl = new FUrl();
        //noinspection NullableProblems
        FUrl.scanUrl(null);
        assertEquals("", FUrl.getUrlNormalizedForGrouping());
    }

    @Test
    public void testNormalizeForGrouping() throws IOException {
        String[][] data = {

                // empty stuff is discarded
                {"", ""},
                {"-", ""},
                {"  - ", ""},

                // trimming should happen
                {"  \t \t \t\t   http://example.com \t  ", "example.com" + TRAILING_SLASH},

                // remove the protocol... anything up to and including the first //
                {"http://example.com", "example.com" + TRAILING_SLASH},
                {"HTTP://example.com", "example.com" + TRAILING_SLASH},
                {"https://example.com", "example.com" + TRAILING_SLASH},
                {"HTTPS://example.com", "example.com" + TRAILING_SLASH},
                {"ftp://example.com", "example.com" + TRAILING_SLASH},
                {"http://foobar.com/agg/bins/detail_page.asp?cmp_id=1860&cid=2441&member_list=yes", "foobar.com/agg/bins/detail_page.asp?cmp_id=1860&cid=2441&member_list=yes"},
                {"https://foobar.com/agg/bins/detail_page.asp?cmp_id=1860&cid=2441&member_list=yes", "foobar.com/agg/bins/detail_page.asp?cmp_id=1860&cid=2441&member_list=yes"},
                {"foobar.com/agg/bins/detail_page.asp?cmp_id=1860&cid=2441&member_list=yes", "foobar.com/agg/bins/detail_page.asp?cmp_id=1860&cid=2441&member_list=yes"},

                // some of the following tests come from http://en.wikipedia.org/wiki/URL_normalization

                // Converting hostname to lower case.
                {"HTTP://Example.COM/", "example.com" + TRAILING_SLASH},
                {"NewYork.kijiji.com/lowUP", "newyork.kijiji.com/lowUP"},
                {"NewYork.kijiji.com:80/lowUP", "newyork.kijiji.com/lowUP"},
                {"NewYork.kijiji.com:443/lowUP", "newyork.kijiji.com/lowUP"},
                {"NewYork.kijiji.com/lowUP:80", "newyork.kijiji.com/lowUP:80"},
                {"NewYork.kijiji.com/lowUP:443", "newyork.kijiji.com/lowUP:443"},

                // remove leading and trailing '.' characters from hostname
                {"z.com./d", "z.com/d"},
                {".z.com/d", "z.com/d"},
                {".z.com./d", "z.com/d"},
                {"z.com.", "z.com" + TRAILING_SLASH},
                {".z.com", "z.com" + TRAILING_SLASH},
                {".z.com.", "z.com" + TRAILING_SLASH},
                {".....z.com...", "z.com" + TRAILING_SLASH},

                // Converting host to lower case.
                {"HTTP://www.Example.com/", "example.com" + TRAILING_SLASH},
                {"HTTP://www.Example.com", "example.com" + TRAILING_SLASH},

                // (a part of a hostname is what is between the dots.)
                // if there are 3 or more parts, and the first part starts with "www", drop the first part.
                {"http://www.example.com/", "example.com" + TRAILING_SLASH},
                {"http://www2.example.com/", "example.com" + TRAILING_SLASH},
                {"http://www453.example.com/", "example.com" + TRAILING_SLASH},
                {"http://www2q.example.com/", "example.com" + TRAILING_SLASH},
                {"http://www.com/", "www.com" + TRAILING_SLASH},
                {"http://www99.com/", "www99.com" + TRAILING_SLASH},
                {"http://www.co.uk/", "co.uk" + TRAILING_SLASH}, // we lose this domain name based on our rule, too bad.
                {"http://www.example.co.uk/", "example.co.uk" + TRAILING_SLASH},

                // Get rid of any trailing anchor
                {"http://www.example.com/abc/def/ghi.html?zzz=1&ppp=2#loc", "example.com/abc/def/ghi.html?zzz=1&ppp=2"},

                // Has an anchor, which should be removed
                {"abc.com/#p", "abc.com" + TRAILING_SLASH},

                // get rid of unnecessary dots
                {"q.com/ab/././cd/../ef/gh", "q.com/ab/ef/gh"},

                // Capitalizing letters in escape sequences.
                // All letters within a percent-encoding triplet (e.g., "%3A") are case-insensitive, and should be capitalized.
                {"http://www.example.com/a%c2%b1b", "example.com/a%C2%B1b"},
                {"http://www.example.com/a%c2%b1b?q%c2=z%c2rr", "example.com/a%C2%B1b?q%C2=z%C2rr"},
                {"http://www.example.com/a%c2%b1b?q%c2=z%c2rr#a%c2b", "example.com/a%C2%B1b?q%C2=z%C2rr"},

                // do not append a trailing slash if there is something in the path
                {"abc.com/qqq/xyz", "abc.com/qqq/xyz"},

                // don't remove the trailing slash if it's present
                {"abc.com/z/", "abc.com/z/"},

                // Adding trailing / Directories are indicated with a trailing slash and should be included in URLs.
                // (This depends on what we have decided to do with regards to this rule.)
                {"http://www.example.com", "example.com" + TRAILING_SLASH},

                // Removing the default port. The default port (port 80 for the “http” scheme) may be removed from a URL.
                {"http://www.example.com:80/bar.html", "example.com/bar.html"},

                // Removing dot-segments. The segments “..” and “.” are usually removed from a URL according
                // to the algorithm described in RFC 3986 (or a similar algorithm).
                // Note that if the path starts with "../" the "../" is preserved. This is just the way java.net.URI works.
                {"http://www.example.com/a/b/../c/./d.html", "example.com/a/c/d.html"},

                // Removing directory index. Default directory indexes are generally not needed in URLs.
                {"http://www.example.com/default.asp", "example.com" + TRAILING_SLASH},
                {"http://www.example.com/index.html", "example.com" + TRAILING_SLASH},
                {"http://www.example.com/a/default.asp", "example.com/a/"},
                {"http://www.example.com/a/index.html", "example.com/a/"},

                // Removing the fragment (anchor). The fragment component of a URL is usually removed.
                {"http://www.example.com/bar.html#section1", "example.com/bar.html"},
                {"http://www.newyorker.com/humor/issuecartoons/2010/07/12/cartoons_20100705#slide=1", "newyorker.com/humor/issuecartoons/2010/07/12/cartoons_20100705"},

                // Removing duplicate slashes Paths which include two adjacent slashes should be converted to one.
                {"http://www.example.com/foo//bar.html", "example.com/foo/bar.html"},
                {"q.com/ab/cd//ef/gh///////////ij/k.html", "q.com/ab/cd/ef/gh/ij/k.html"},

                // Removing the "?" when the querystring is empty. When the querystring is empty, there is no need for the "?".
                {"http://www.example.com/display?", "example.com/display"},

                // weird URLs
                {"HTTP://WWW.FXBRIDGE.COM/", "fxbridge.com" + TRAILING_SLASH},
                {"http: //global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation", "global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation"},
                {"http:     //global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation", "global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation"},
                {"Http: //global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation/", "global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation/"},
                {"Http://global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation", "global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation"},
                {"HTTP://global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation", "global-labour-issues.suite101.com/article.cfm/panacea_or_worker_exploitation"},
                {"blockedReferrer", "blockedreferrer" + TRAILING_SLASH},
                {"http://abc.com/file/", "abc.com/file/"},
                {"http:// wineaccess.ca/slasher", "wineaccess.ca/slasher"},
                {"http://          wineaccess.ca/slasher", "wineaccess.ca/slasher"},
                {"http: // wineaccess.ca/slasher", "wineaccess.ca/slasher"},
                {"http:  //  wineaccess.ca/slasher", "wineaccess.ca/slasher"},
        };

        FUrl FUrl = new FUrl();
        runNormalizerTests(FUrl, data);
    }

    private void runNormalizerTests(FUrl FUrl, String[][] data) throws IOException {
        for (String[] testData : data) {

            // here's the data
            String rawUrl = testData[0];
            String expectedNormalizedUrl = testData[1];

            System.out.println("Trying: [" + rawUrl + "]");

            // scan the URL
            FUrl.scanUrl(rawUrl);
            String actualNormalizedUrl = FUrl.getUrlNormalizedForGrouping();

            // check the result of the normalization
            assertEquals("Unexpected output", expectedNormalizedUrl, actualNormalizedUrl);
        }
    }

    @Test
    public void testUrlScanning() throws Exception {
        // try empty strings
        FUrl FUrl = new FUrl();
        String url = "";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "", "", "", "", "", "");

        // happy case
        url = "http://example.com/index.html?qqq=zzz&ppp=rrr#splat=boink";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "http://", "example.com", "", "", "?qqq=zzz&ppp=rrr", "#splat=boink");

        // try without a protocol
        url = "example.com/index.html";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "http://", "example.com", "", TRAILING_SLASH, "", "");

        // try without a '.' character
        url = "example";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "http://", "example", "", TRAILING_SLASH, "", "");

        // try with only a single / character
        url = "http:/example.com";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "http://", "http", "", "/example.com", "", "");

        // try with three '/' characters for the protocol
        url = "file:///file.txt";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "file://", "", "", "/file.txt", "", "");

        // try with the :// but nothing afterwards (a degenerate case)
        url = "http://";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "", "", "", "", "", "");

        // try with the :/ but nothing afterwards (a degenerate case)
        url = "http:/";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "", "", "", "", "", "");

        // try with the : but nothing afterwards (a degenerate case)
        url = "http:";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "", "", "", "", "", "");

        // try with an anchor segment
        url = "abc.com/#w";
        FUrl.scanUrl(url);
        verifySegments(url, FUrl, "http://", "abc.com", "", TRAILING_SLASH, "", "#w");

        // check the hostname segment, but without the "www." part.
        url = "www.z.com";
        FUrl.scanUrl(url);
        assertEquals("didn't match", "z.com", FUrl.getSegmentHostnameWithoutPrecedingWww());

        // check the hostname segment, but without the "www45." part.
        url = "www45.z.com";
        FUrl.scanUrl(url);
        assertEquals("didn't match", "z.com", FUrl.getSegmentHostnameWithoutPrecedingWww());
    }

    private void verifySegments(String url, FUrl FUrl, String scheme, String hostname, String port, String path, String query, String anchor) {
        assertEquals("For [" + url + "], scheme doesn't match", scheme, FUrl.getSegmentScheme());
        assertEquals("For [" + url + "], hostname doesn't match", hostname, FUrl.getSegmentHostname());
        assertEquals("For [" + url + "], port doesn't match", port, FUrl.getSegmentPort());
        assertEquals("For [" + url + "], path doesn't match", path, FUrl.getSegmentPath());
        assertEquals("For [" + url + "], query doesn't match", query, FUrl.getSegmentQuery());
        assertEquals("For [" + url + "], anchor doesn't match", anchor, FUrl.getSegmentAnchor());
    }

    @Test
    public void testUrlShouldBeDiscarded() throws Exception {
        String url = "";
        FUrl FUrl = new FUrl();
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "-";
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "file://example.com";
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "file:///example.com";
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "file:///C:/jsox/10%20Essential%20Business%20Leadership%20Skills.htm";
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "http://example/whatever.html";
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "example/whatever.html";
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "http://123.45.12.6/index.html";
        FUrl.scanUrl(url);
        assertTrue(FUrl.urlShouldBeDiscarded());

        url = "http://blah.com/127.0.0.1/blue";
        FUrl.scanUrl(url);
        assertFalse(FUrl.urlShouldBeDiscarded());

        url = "http://abc.com:80/index.html?abc=def&qqq=rrr";
        FUrl.scanUrl(url);
        assertFalse(FUrl.urlShouldBeDiscarded());
    }

    @Test
    public void testLinkable() throws IOException {
        FUrl tool = new FUrl();

        // column 1 = raw, column 2 = non-linkable, column 3 = linkable
        final String[][] data = {
                {"z.com/index.html", "z.com" + TRAILING_SLASH, "http://z.com/index.html"},
                {"httP://abC.com:80/qqq/rrr.html", "abc.com/qqq/rrr.html", "http://abc.com/qqq/rrr.html"},
                {"httP://abC.com:5555/qqq/rrr.html", "abc.com/qqq/rrr.html", "http://abc.com:5555/qqq/rrr.html"},
                {"abC.com/qqq/rrr.html", "abc.com/qqq/rrr.html", "http://abc.com/qqq/rrr.html"},
        };

        for (String[] testData : data) {

            // here's the data
            String rawUrl = testData[0];
            String expectedNonLinkableUrl = testData[1];
            String expectedLinkableUrl = testData[2];

            System.out.println("Trying: [" + rawUrl + "]");

            // scanUrl both linkable and nonLinkable.
            tool.scanUrl(rawUrl);
            String nonLinkableUrl = tool.getUrlNormalizedForGrouping();
            String linkableUrl = tool.getUrlNormalizedAndLinkable();

            // check the result of the normalization
            assertEquals("Unexpected non-linkable output", expectedNonLinkableUrl, nonLinkableUrl);
            assertEquals("Unexpected linkable output", expectedLinkableUrl, linkableUrl);
        }
    }

    @Test
    public void testOmittingDocumentUntilEnd() throws IOException {
        FUrl tool = new FUrl();

        // Note that the trailing slash issue here is somewhat arbitrary.

        // column 1 = raw, column 2 = normal, column 3 = omitting
        final String[][] data = {
                {"httP://abCa.com:80/qqq/rrr.html", "abca.com/qqq/rrr.html", "abca.com/qqq/"},
                {"httP://abCb.com:5555/qqq/rrr.html", "abcb.com/qqq/rrr.html", "abcb.com/qqq/"},
                {"abCc.com/qqq/rrr.html", "abcc.com/qqq/rrr.html", "abcc.com/qqq/"},
                {"abcd.com", "abcd.com" + TRAILING_SLASH, "abcd.com" + TRAILING_SLASH},
                {"abce.com/", "abce.com" + TRAILING_SLASH, "abce.com" + TRAILING_SLASH},
                {"abcf.com:80", "abcf.com" + TRAILING_SLASH, "abcf.com" + TRAILING_SLASH},
                {"abcg.com/z.txt", "abcg.com/z.txt", "abcg.com"},
                {"abch.com/?rty=4&mkl=12", "abch.com?rty=4&mkl=12", "abch.com"},
                {"abci.com/#anchor", "abci.com" + TRAILING_SLASH, "abci.com" + TRAILING_SLASH},
                {"a.com/xyz", "a.com/xyz", "a.com"},
        };

        for (String[] testData : data) {

            // here's the data
            String rawUrl = testData[0];
            String expectedNormal = testData[1];
            String expectedOmitting = testData[2];

            System.out.println("Trying: [" + rawUrl + "]");

            // scanUrl both
            tool.scanUrl(rawUrl);
            String normalUrl = tool.getUrlNormalizedForGrouping();
            String omittingUrl = tool.getUrlNormalizedForGroupingByPath();

            // check the result of the normalization
            assertEquals("Unexpected normal normalized output", expectedNormal, normalUrl);
            assertEquals("Unexpected omitting normalized output", expectedOmitting, omittingUrl);
        }
    }

    @Test
    public void testFindIndexOfToken() throws IOException {
        FUrl tool = new FUrl();
        tool.scanUrl("z.blah123456blah.com");
        assertEquals(4, tool.findIndexOfToken("a", 2, 14));
        assertEquals(-1, tool.findIndexOfToken("ape", 2, 14));
        assertEquals(2, tool.findIndexOfToken("blah", 2, 14));
        assertEquals(2, tool.findIndexOfToken("blah", 2, 14));
        assertEquals(11, tool.findIndexOfToken("6blah", 2, 14));
    }

    @Test
    public void testCharacterHelperMethods() {
        final String numeric = "0123456789";
        final String uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        final String lowercase = "abcdefghijklmnopqrstuvwxyz";
        final String alphabetic = uppercase + lowercase;
        final String alphanumeric = numeric + alphabetic;
        for (char c = 0; c < 256; c++) {
            assertEquals("Not numeric", numeric.contains(String.valueOf(c)), FUrl.isNumeric(c));
            assertEquals("Not alphabetic", alphabetic.contains(String.valueOf(c)), FUrl.isAlphabetic(c));
            assertEquals("Not alphanumeric", alphanumeric.contains(String.valueOf(c)), FUrl.isAlphanumeric(c));
            assertFalse("Not lowercase", uppercase.contains(String.valueOf(FUrl.toLower(c))));
            assertFalse("Not uppercase", lowercase.contains(String.valueOf(FUrl.toUpper(c))));
        }
    }

    // One way that this was implemented was to adjust the offsets to skip the preceding "www." of a hostname,
    // but this has a side effect: subsequent assemblies will be missing those characters, even if they are desired.
    @Test
    public void testNonMunging() throws IOException {
        FUrl tool = new FUrl();
        String raw = "http://www.z.com/blah.html";
        tool.scanUrl(raw);

        assertEquals("hostname should match", "www.z.com", tool.getSegmentHostname());

        // this should not remove the "www." from the set of offsets
        tool.getUrlNormalizedForGrouping();

        // get the current hostname, as per the offsets
        String hostnameReturned = tool.getSegmentHostname();
        assertEquals("The www. should still be there", "www.z.com", hostnameReturned);

        // now try removing the document from the path
        raw = "z.com/abc/g.txt";
        tool.scanUrl(raw);

        // the path should include the document
        assertEquals("path should match", "/abc/g.txt", tool.getSegmentPath());

        // this should not munge the offsets
        tool.getUrlNormalizedForGroupingByPath();

        // the document name should not be gone now
        assertEquals("path should match", "/abc/g.txt", tool.getSegmentPath());
    }

    @Test
    public void testGetSegmentDocument() throws IOException {
        FUrl tool = new FUrl();

        String raw = "z.com";
        tool.scanUrl(raw);
        assertEquals("Document not as expected, supposed to be empty", "", tool.getSegmentDocument());

        raw = "http://www.z.com:99/zoobie/blah.html?ab=de";
        tool.scanUrl(raw);
        assertEquals("Document not as expected", "blah.html", tool.getSegmentDocument());
    }

    @Test
    public void testGetSegmentPathWithoutDocument() throws IOException {
        FUrl tool = new FUrl();

        String raw = "z.com";
        tool.scanUrl(raw);
        assertEquals("Path not as expected, supposed to be empty", TRAILING_SLASH, tool.getSegmentPathWithoutDocument());

        raw = "http://www.z.com:99/zoobie/splat/blah.html?ab=de";
        tool.scanUrl(raw);
        assertEquals("Path not as expected", "/zoobie/splat/", tool.getSegmentPathWithoutDocument());

        // For this one, the program cannot distinguish between a document and a path, so it assumes document.
        raw = "http://www.z.com/zoobie/splat";
        tool.scanUrl(raw);
        assertEquals("Path not as expected", "/zoobie/", tool.getSegmentPathWithoutDocument());
    }

    @Test
    public void testScanReturnValue() throws IOException {
        FUrl tool = new FUrl();
        assertEquals("Should get port", ":999", tool.scanUrl("z.com:999").getSegmentPort());
    }

    @Test
    public void testReversedHostnames() throws IOException {
        FUrl tool = new FUrl();

        // some basic tests around this functionality
        assertEquals("Should get reversed hostname", "com.def.abc", tool.scanUrl("http://abc.def.Com/blah.html").getSegmentHostnameReversed());
        assertEquals("Should get reversed hostname", "com.def.abc", tool.scanUrl("http://www.abc.def.Com/blah.html").getSegmentHostnameReversed());
        assertEquals("Should get reversed hostname", "com.def.abc", tool.scanUrl("abc.def.com").getSegmentHostnameReversed());
        assertEquals("Should get reversed hostname", "com.def.abc", tool.scanUrl("http://abc.def.Com/blah.html").getSegmentHostnameReversed());
        assertEquals("Should get reversed hostname", "com.def.abc", tool.scanUrl("abc.def.com:8080/blah.html").getSegmentHostnameReversed());

        // here's one with only a single word for the hostname
        assertEquals("Should get reversed hostname", "blargh", tool.scanUrl("blargh").getSegmentHostnameReversed());

        // now try some boundary testing around this
        int urlBufferSize = 2048;
        int assemblyBufferSize = 20;
        tool = new FUrl(urlBufferSize, assemblyBufferSize);

        // shorter than half the buffer size
        assertEquals("Should get reversed hostname", "com.def", tool.scanUrl("def.com").getSegmentHostnameReversed());

        // size is 9 (one less than half)
        assertEquals("Should get reversed hostname", "com.def.a", tool.scanUrl("a.def.com").getSegmentHostnameReversed());

        // size is 10 (half)
        assertEquals("Should get reversed hostname", "com.def.ab", tool.scanUrl("ab.def.com").getSegmentHostnameReversed());

        // size is 11 (one more than half)
        assertEquals("Should get reversed hostname", "com.def.bc", tool.scanUrl("abc.def.com").getSegmentHostnameReversed());

        // now try something larger (15)
        assertEquals("Should get reversed hostname", "com.def.bc", tool.scanUrl("xyz.abc.def.com").getSegmentHostnameReversed());

        // size is 19 (one less than the max)
        assertEquals("Should get reversed hostname", "com.def.bc", tool.scanUrl("xyz1234.abc.def.com").getSegmentHostnameReversed());

        // size is 20 (the max)
        assertEquals("Should get reversed hostname", "com.def.bc", tool.scanUrl("xyz12345.abc.def.com").getSegmentHostnameReversed());

        // size is 21 (more than the max)
        assertEquals("Should get reversed hostname", "co.def.abc", tool.scanUrl("xyz123456.abc.def.com").getSegmentHostnameReversed());
    }

    @Test
    public void testNormalizeWithReversedHostnames() throws IOException {
        FUrl tool = new FUrl();

        // some basic tests around this functionality
        assertEquals("Should get reversed url", "com.def.abc/Blah.html", tool.scanUrl("http://abc.def.Com/Blah.html").getUrlNormalizedForGroupingReversingHostname());
        assertEquals("Should get reversed url", "com.def.abc/blah.html", tool.scanUrl("http://www.abc.def.Com/blah.html").getUrlNormalizedForGroupingReversingHostname());
        assertEquals("Should get reversed url", "com.def.abc" + TRAILING_SLASH, tool.scanUrl("abc.def.com").getUrlNormalizedForGroupingReversingHostname());
        assertEquals("Should get reversed url", "com.def.abc/blah.html", tool.scanUrl("http://abc.def.Com/blah.html").getUrlNormalizedForGroupingReversingHostname());
        assertEquals("Should get reversed url", "com.def.abc/blah.html", tool.scanUrl("abc.def.com:8080/blah.html").getUrlNormalizedForGroupingReversingHostname());

        // here's one with only a single word for the hostname
        assertEquals("Should get reversed url", "blargh" + TRAILING_SLASH, tool.scanUrl("blargh").getUrlNormalizedForGroupingReversingHostname());

        // now try some boundary testing around this
        int urlBufferSize = 2048;
        int assemblyBufferSize = 20;
        tool = new FUrl(urlBufferSize, assemblyBufferSize);

        // shorter than half the buffer size
        assertEquals("Should get reversed url", "com.def" + TRAILING_SLASH, tool.scanUrl("def.com").getUrlNormalizedForGroupingReversingHostname());

        // size is 9 (one less than half)
        assertEquals("Should get reversed url", "com.def.a" + TRAILING_SLASH, tool.scanUrl("a.def.com").getUrlNormalizedForGroupingReversingHostname());

        // size is 10 (half)
        assertEquals("Should get reversed url", "com.def.ab" + TRAILING_SLASH, tool.scanUrl("ab.def.com").getUrlNormalizedForGroupingReversingHostname());

        // size is 11 (one more than half)
        assertEquals("Should get reversed url", "com.def.bc" + TRAILING_SLASH, tool.scanUrl("abc.def.com").getUrlNormalizedForGroupingReversingHostname());

        // now try something larger (15)
        assertEquals("Should get reversed url", "com.def.bc" + TRAILING_SLASH, tool.scanUrl("xyz.abc.def.com").getUrlNormalizedForGroupingReversingHostname());

        // size is 19 (one less than the max)
        assertEquals("Should get reversed url", "com.def.bc" + TRAILING_SLASH, tool.scanUrl("xyz1234.abc.def.com").getUrlNormalizedForGroupingReversingHostname());

        // size is 20 (the max)
        assertEquals("Should get reversed url", "com.def.bc" + TRAILING_SLASH, tool.scanUrl("xyz12345.abc.def.com").getUrlNormalizedForGroupingReversingHostname());

        // size is 21 (more than the max)
        assertEquals("Should get reversed url", "co.def.abc" + TRAILING_SLASH, tool.scanUrl("xyz123456.abc.def.com").getUrlNormalizedForGroupingReversingHostname());
    }

    @Test
    public void testGetUrlNormalizedAsRawAsPossible() throws IOException {
        FUrl tool = new FUrl();

        // This should test the basic case, just get everything back
        String raw = "htTP://www.Z.com:81/abc/../deg/ghi/blah.html?a=1&b=2#qqq";
        tool.scanUrl(raw);
        assertEquals("should match", "http://www.z.com:81/deg/ghi/blah.html?a=1&b=2#qqq", tool.getUrlNormalizedAsRawAsPossible());

        // Test that it drops port 80
        raw = "htTP://www.Z.com:80/abc/../deg/ghi/blah.html?a=1&b=2#qqq";
        tool.scanUrl(raw);
        assertEquals("should match", "http://www.z.com/deg/ghi/blah.html?a=1&b=2#qqq", tool.getUrlNormalizedAsRawAsPossible());
    }

    @Test
    public void testHostnameIsIpAddress() throws IOException {
        FUrl tool = new FUrl();
        tool.scanUrl("http://99baloons.com/cool.html");
        assertFalse(tool.hostnameIsIpAddress());

        tool.scanUrl("http://12.13.14.15/cool.html");
        assertTrue(tool.hostnameIsIpAddress());

        tool.scanUrl("");
        assertFalse(tool.hostnameIsIpAddress());
    }

}

