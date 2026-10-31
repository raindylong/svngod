package server

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestPropfindRevProps(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><log xmlns="http://subversion.tigris.org/xmlns/svn/"/></prop></propfind>`
	req, _ := http.NewRequest("PROPFIND", base+"/svn/demo/!svn/rev/1", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/xml")
	req.Header.Set("Depth", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `<log xmlns="http://subversion.tigris.org/xmlns/svn/">initial import</log>`) {
		t.Fatalf("revprop missing from PROPFIND:\n%s", b)
	}
}
func TestPropfindAllRevProps(t *testing.T) {
	ts, _, base := testServer(t, "", "")
	defer ts.Close()
	body := `<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><allprop xmlns="DAV:"/></propfind>`
	req, _ := http.NewRequest("PROPFIND", base+"/svn/demo/!svn/rev/1", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/xml")
	req.Header.Set("Depth", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`<log xmlns="http://subversion.tigris.org/xmlns/svn/">initial import</log>`,
		`<author xmlns="http://subversion.tigris.org/xmlns/svn/">alice</author>`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("allprop missing %q:\n%s", want, b)
		}
	}
}
