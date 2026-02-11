package pq

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

func TestCopyInStmt(t *testing.T) {
	tests := []struct {
		inTable string
		inCols  []string
		want    string
	}{
		{`table name`, nil, `COPY "table name" FROM STDIN`},
		{"table name", []string{"column 1", "column 2"}, `COPY "table name" ("column 1", "column 2") FROM STDIN`},
		{`table " name """`, []string{`co"lumn""`}, `COPY "table "" name """"""" ("co""lumn""""") FROM STDIN`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			have := CopyIn(tt.inTable, tt.inCols...)
			if have != tt.want {
				t.Fatalf("\nhave: %q\nwant: %q", have, tt.want)
			}
		})
	}
}

func TestCopyInSchemaStmt(t *testing.T) {
	tests := []struct {
		inSchema string
		inTable  string
		inCols   []string
		want     string
	}{
		{"schema name", "table name", nil,
			`COPY "schema name"."table name" FROM STDIN`},

		{"schema name", "table name", []string{"column 1", "column 2"},
			`COPY "schema name"."table name" ("column 1", "column 2") FROM STDIN`},

		{`schema " name """`, `table " name """`, []string{`co"lumn""`},
			`COPY "schema "" name """""""."table "" name """"""" ("co""lumn""""") FROM STDIN`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			have := CopyInSchema(tt.inSchema, tt.inTable, tt.inCols...)
			if have != tt.want {
				t.Fatalf("\nhave: %q\nwant: %q", have, tt.want)
			}
		})
	}
}

func TestCopyInMultipleValues(t *testing.T) {
	// "It is important to note that although the direct connections and
	// Supavisor in session mode support prepared statements, Supavisor in
	// transaction mode does not."
	// https://supabase.com/docs/guides/troubleshooting/disabling-prepared-statements-qL8lEL
	pqtest.SkipSupavisorTransactionMode(t)

	tests := []struct {
		cols []string
	}{
		{[]string{"a", "b"}},
		{nil},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
			t.Parallel()
			db := pqtest.MustDB(t)

			tx := pqtest.Begin(t, db)
			defer tx.Rollback()

			pqtest.Exec(t, tx, `create temp table tbl (a int, b varchar)`)
			stmt := pqtest.Prepare(t, tx, CopyIn("tbl", tt.cols...))

			str := strings.Repeat("#", 500)
			for i := 0; i < 500; i++ {
				_, err := stmt.Exec(int64(i), str)
				if err != nil {
					t.Fatal(err)
				}
			}

			r, err := stmt.Exec()
			if err != nil {
				t.Fatal(err)
			}
			rows, err := r.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if rows != 500 {
				t.Fatalf("expected 500 rows affected, not %d", rows)
			}

			if err := stmt.Close(); err != nil {
				t.Fatal(err)
			}

			var num int
			err = tx.QueryRow("select count(*) from tbl").Scan(&num)
			if err != nil {
				t.Fatal(err)
			}
			if num != 500 {
				t.Fatalf("expected 500 items, not %d", num)
			}
		})
	}
}

func TestCopyInRaiseStmtTrigger(t *testing.T) {
	// Transaction mode connection pooling breaks parallel tests using COPY.
	if !pqtest.SupavisorTransactionMode() {
		t.Parallel()
	}
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMP TABLE temp (a int, b varchar)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = txn.Exec(`
			CREATE OR REPLACE FUNCTION pg_temp.temptest()
			RETURNS trigger AS
			$BODY$ begin
				raise notice 'Hello world';
				return new;
			end $BODY$
			LANGUAGE plpgsql`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = txn.Exec(`
			CREATE TRIGGER temptest_trigger
			BEFORE INSERT
			ON temp
			FOR EACH ROW
			EXECUTE PROCEDURE pg_temp.temptest()`)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(CopyIn("temp", "a", "b"))
	if err != nil {
		t.Fatal(err)
	}

	longString := strings.Repeat("#", 500)

	_, err = stmt.Exec(int64(1), longString)
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	var num int
	err = txn.QueryRow("SELECT COUNT(*) FROM temp").Scan(&num)
	if err != nil {
		t.Fatal(err)
	}

	if num != 1 {
		t.Fatalf("expected 1 items, not %d", num)
	}
}

func TestCopyInTypes(t *testing.T) {
	// Transaction mode connection pooling breaks parallel tests using COPY.
	if !pqtest.SupavisorTransactionMode() {
		t.Parallel()
	}
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMP TABLE temp (num INTEGER, text VARCHAR, blob BYTEA, nothing VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(CopyIn("temp", "num", "text", "blob", "nothing"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec(int64(1234567890), "Héllö\n ☃!\r\t\\", []byte{0, 255, 9, 10, 13}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	var num int
	var text string
	var blob []byte
	var nothing sql.NullString

	err = txn.QueryRow("SELECT * FROM temp").Scan(&num, &text, &blob, &nothing)
	if err != nil {
		t.Fatal(err)
	}

	if num != 1234567890 {
		t.Fatal("unexpected result", num)
	}
	if text != "Héllö\n ☃!\r\t\\" {
		t.Fatal("unexpected result", text)
	}
	if !bytes.Equal(blob, []byte{0, 255, 9, 10, 13}) {
		t.Fatal("unexpected result", blob)
	}
	if nothing.Valid {
		t.Fatal("unexpected result", nothing.String)
	}
}

func TestCopyInWrongType(t *testing.T) {
	// Transaction mode connection pooling breaks parallel tests using COPY.
	if !pqtest.SupavisorTransactionMode() {
		t.Parallel()
	}
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMP TABLE temp (num INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(CopyIn("temp", "num"))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec("Héllö\n ☃!\r\t\\")
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec()
	if err == nil {
		t.Fatal("expected error")
	}
	if pge := err.(*Error); pge.Code.Name() != "invalid_text_representation" {
		t.Fatalf("expected 'invalid input syntax for integer' error, got %s (%+v)", pge.Code.Name(), pge)
	}
}

func TestCopyOutsideOfTxnError(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Prepare(CopyIn("temp", "num"))
	if err == nil {
		t.Fatal("COPY outside of transaction did not return an error")
	}
	if err != errCopyNotSupportedOutsideTxn {
		t.Fatalf("expected %s, got %s", err, err.Error())
	}
}

func TestCopyInBinaryError(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMP TABLE temp (num INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = txn.Prepare("COPY temp (num) FROM STDIN WITH binary")
	if err != errBinaryCopyNotSupported {
		t.Fatalf("expected %s, got %+v", errBinaryCopyNotSupported, err)
	}
	// check that the protocol is in a valid state
	err = txn.Rollback()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCopyFromError(t *testing.T) {
	// Transaction mode connection pooling breaks parallel tests using COPY.
	if !pqtest.SupavisorTransactionMode() {
		t.Parallel()
	}
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMP TABLE temp (num INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = txn.Prepare("-- comment\n  /* comment */  COPY temp (num) TO STDOUT")
	if err != errCopyToNotSupported {
		t.Fatalf("expected %s, got %+v", errCopyToNotSupported, err)
	}
	// check that the protocol is in a valid state
	err = txn.Rollback()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCopySyntaxError(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Prepare("COPY ")
	if err == nil {
		t.Fatal("expected error")
	}
	if pge := err.(*Error); pge.Code.Name() != "syntax_error" {
		t.Fatalf("expected syntax error, got %s (%+v)", pge.Code.Name(), pge)
	}
	// check that the protocol is in a valid state
	err = txn.Rollback()
	if err != nil {
		t.Fatal(err)
	}
}

// Tests for connection errors in copyin.resploop()
func TestCopyRespLoopConnectionError(t *testing.T) {
	// This test won't work with transaction mode connection pooling
	// without a transaction.
	pqtest.SkipSupavisorTransactionMode(t)

	t.Parallel()
	db := pqtest.MustDB(t)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()

	var pid int
	err = txn.QueryRow("SELECT pg_backend_pid()").Scan(&pid)
	if err != nil {
		t.Fatal(err)
	}

	_, err = txn.Exec("CREATE TEMP TABLE temp (a int)")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(CopyIn("temp", "a"))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, err = db.Exec("SELECT pg_terminate_backend($1)", pid)
	if err != nil {
		t.Fatal(err)
	}

	retry(t, time.Second*5, func() error {
		_, err = stmt.Exec()
		if err == nil {
			return fmt.Errorf("expected error")
		}
		return nil
	})
	switch pge := err.(type) {
	case *Error:
		if pge.Code.Name() != "admin_shutdown" {
			t.Fatalf("expected admin_shutdown, got %s", pge.Code.Name())
		}
	case *net.OpError:
		// ignore
	default:
		if err == driver.ErrBadConn {
			// likely an EPIPE
		} else if err == errCopyInClosed {
			// ignore
		} else {
			t.Fatalf("unexpected error, got %+#v", err)
		}
	}

	_ = stmt.Close()
}

// retry executes f in a backoff loop until it doesn't return an error. If this
// doesn't happen within duration, t.Fatal is called with the latest error.
func retry(t *testing.T, duration time.Duration, f func() error) {
	start := time.Now()
	next := time.Millisecond * 100
	for {
		err := f()
		if err == nil {
			return
		}
		if time.Since(start) > duration {
			t.Fatal(err)
		}
		time.Sleep(next)
		next *= 2
	}
}

func BenchmarkCopyIn(b *testing.B) {
	db := pqtest.MustDB(b)

	txn, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Rollback()

	_, err = txn.Exec("CREATE TEMP TABLE temp (a int, b varchar)")
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := txn.Prepare(CopyIn("temp", "a", "b"))
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, err = stmt.Exec(int64(i), "hello world!")
		if err != nil {
			b.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		b.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		b.Fatal(err)
	}

	var num int
	err = txn.QueryRow("SELECT COUNT(*) FROM temp").Scan(&num)
	if err != nil {
		b.Fatal(err)
	}

	if num != b.N {
		b.Fatalf("expected %d items, not %d", b.N, num)
	}
}

func BenchmarkCopy(b *testing.B) {
	bigTableColumns := []string{"ABIOGENETICALLY", "ABORIGINALITIES", "ABSORBABILITIES", "ABSORBEFACIENTS", "ABSORPTIOMETERS", "ABSTRACTIONISMS",
		"ABSTRACTIONISTS", "ACANTHOCEPHALAN", "ACCEPTABILITIES", "ACCEPTINGNESSES", "ACCESSARINESSES", "ACCESSIBILITIES", "ACCESSORINESSES", "ACCIDENTALITIES",
		"ACCIDENTOLOGIES", "ACCLIMATISATION", "ACCLIMATIZATION", "ACCOMMODATINGLY", "ACCOMMODATIONAL", "ACCOMPLISHMENTS", "ACCOUNTABLENESS", "ACCOUNTANTSHIPS",
		"ACCULTURATIONAL", "ACETOPHENETIDIN", "ACETYLSALICYLIC", "ACHONDROPLASIAS", "ACHONDROPLASTIC", "ACHROMATICITIES", "ACHROMATISATION", "ACHROMATIZATION",
		"ACIDIMETRICALLY", "ACKNOWLEDGEABLE", "ACKNOWLEDGEABLY", "ACKNOWLEDGEMENT", "ACKNOWLEDGMENTS", "ACQUIRABILITIES", "ACQUISITIVENESS", "ACRIMONIOUSNESS",
		"ACROPARESTHESIA", "ACTINOBIOLOGIES", "ACTINOCHEMISTRY", "ACTINOTHERAPIES", "ADAPTABLENESSES", "ADDITIONALITIES", "ADENOCARCINOMAS", "ADENOHYPOPHYSES",
		"ADENOHYPOPHYSIS", "ADENOIDECTOMIES", "ADIATHERMANCIES", "ADJUSTABILITIES", "ADMINISTRATIONS", "ADMIRABLENESSES", "ADMISSIBILITIES", "ADRENALECTOMIES",
		"ADSORBABILITIES", "ADVENTUROUSNESS", "ADVERSARINESSES", "ADVISABLENESSES", "AERODYNAMICALLY", "AERODYNAMICISTS", "AEROELASTICIANS", "AEROHYDROPLANES",
		"AEROLITHOLOGIES", "AEROSOLISATIONS", "AEROSOLIZATIONS", "AFFECTABILITIES", "AFFECTIVENESSES", "AFFORDABILITIES", "AFFRANCHISEMENT", "AFTERSENSATIONS",
		"AGGLUTINABILITY", "AGGRANDISEMENTS", "AGGRANDIZEMENTS", "AGGREGATENESSES", "AGRANULOCYTOSES", "AGRANULOCYTOSIS", "AGREEABLENESSES", "AGRIBUSINESSMAN",
		"AGRIBUSINESSMEN", "AGRICULTURALIST", "AIRWORTHINESSES", "ALCOHOLISATIONS", "ALCOHOLIZATIONS", "ALCOHOLOMETRIES", "ALEXIPHARMAKONS", "ALGORITHMICALLY",
		"ALKALINISATIONS", "ALKALINIZATIONS", "ALLEGORICALNESS", "ALLEGORISATIONS", "ALLEGORIZATIONS", "ALLELOMORPHISMS", "ALLERGENICITIES", "ALLOTETRAPLOIDS",
		"ALLOTETRAPLOIDY", "ALLOTRIOMORPHIC", "ALLOWABLENESSES", "ALPHABETISATION", "ALPHABETIZATION", "ALTERNATIVENESS", "ALTITUDINARIANS", "ALUMINOSILICATE",
		"ALUMINOTHERMIES", "AMARYLLIDACEOUS", "AMBASSADORSHIPS", "AMBIDEXTERITIES", "AMBIGUOUSNESSES", "AMBISEXUALITIES", "AMBITIOUSNESSES", "AMINOPEPTIDASES",
		"AMINOPHENAZONES", "AMMONIFICATIONS", "AMORPHOUSNESSES", "AMPHIDIPLOIDIES", "AMPHITHEATRICAL", "ANACOLUTHICALLY", "ANACREONTICALLY", "ANAESTHESIOLOGY",
		"ANAESTHETICALLY", "ANAGRAMMATISING", "ANAGRAMMATIZING", "ANALOGOUSNESSES", "ANALYZABILITIES", "ANAMORPHOSCOPES", "ANCYLOSTOMIASES", "ANCYLOSTOMIASIS",
		"ANDROGYNOPHORES", "ANDROMEDOTOXINS", "ANDROMONOECIOUS", "ANDROMONOECISMS", "ANESTHETIZATION", "ANFRACTUOSITIES", "ANGUSTIROSTRATE", "ANIMATRONICALLY",
		"ANISOTROPICALLY", "ANKYLOSTOMIASES", "ANKYLOSTOMIASIS", "ANNIHILATIONISM", "ANOMALISTICALLY", "ANOMALOUSNESSES", "ANONYMOUSNESSES", "ANSWERABILITIES",
		"ANTAGONISATIONS", "ANTAGONIZATIONS", "ANTAPHRODISIACS", "ANTEPENULTIMATE", "ANTHROPOBIOLOGY", "ANTHROPOCENTRIC", "ANTHROPOGENESES", "ANTHROPOGENESIS",
		"ANTHROPOGENETIC", "ANTHROPOLATRIES", "ANTHROPOLOGICAL", "ANTHROPOLOGISTS", "ANTHROPOMETRIES", "ANTHROPOMETRIST", "ANTHROPOMORPHIC", "ANTHROPOPATHIES",
		"ANTHROPOPATHISM", "ANTHROPOPHAGIES", "ANTHROPOPHAGITE", "ANTHROPOPHAGOUS", "ANTHROPOPHOBIAS", "ANTHROPOPHOBICS", "ANTHROPOPHUISMS", "ANTHROPOPSYCHIC",
		"ANTHROPOSOPHIES", "ANTHROPOSOPHIST", "ANTIABORTIONIST", "ANTIALCOHOLISMS", "ANTIAPHRODISIAC", "ANTIARRHYTHMICS", "ANTICAPITALISMS", "ANTICAPITALISTS",
		"ANTICARCINOGENS", "ANTICHOLESTEROL", "ANTICHOLINERGIC", "ANTICHRISTIANLY", "ANTICLERICALISM", "ANTICLIMACTICAL", "ANTICOINCIDENCE", "ANTICOLONIALISM",
		"ANTICOLONIALIST", "ANTICOMPETITIVE", "ANTICONVULSANTS", "ANTICONVULSIVES", "ANTIDEPRESSANTS", "ANTIDERIVATIVES", "ANTIDEVELOPMENT", "ANTIEDUCATIONAL",
		"ANTIEGALITARIAN", "ANTIFASHIONABLE", "ANTIFEDERALISTS", "ANTIFERROMAGNET", "ANTIFORECLOSURE", "ANTIHELMINTHICS", "ANTIHISTAMINICS", "ANTILIBERALISMS",
		"ANTILIBERTARIAN", "ANTILOGARITHMIC", "ANTIMATERIALISM", "ANTIMATERIALIST", "ANTIMETABOLITES", "ANTIMILITARISMS", "ANTIMILITARISTS", "ANTIMONARCHICAL",
		"ANTIMONARCHISTS", "ANTIMONOPOLISTS", "ANTINATIONALIST", "ANTINUCLEARISTS", "ANTIODONTALGICS", "ANTIPERISTALSES", "ANTIPERISTALSIS", "ANTIPERISTALTIC",
		"ANTIPERSPIRANTS", "ANTIPHLOGISTICS", "ANTIPORNOGRAPHY", "ANTIPROGRESSIVE", "ANTIQUARIANISMS", "ANTIRADICALISMS", "ANTIRATIONALISM", "ANTIRATIONALIST",
		"ANTIRATIONALITY", "ANTIREPUBLICANS", "ANTIROMANTICISM", "ANTISEGREGATION", "ANTISENTIMENTAL", "ANTISEPARATISTS", "ANTISEPTICISING", "ANTISEPTICIZING",
		"ANTISEXUALITIES", "ANTISHOPLIFTING", "ANTISOCIALITIES", "ANTISPECULATION", "ANTISPECULATIVE", "ANTISYPHILITICS", "ANTITHEORETICAL", "ANTITHROMBOTICS",
		"ANTITRADITIONAL", "ANTITRANSPIRANT", "ANTITRINITARIAN", "ANTITUBERCULOUS", "ANTIVIVISECTION", "APHELIOTROPISMS", "APOCALYPTICALLY", "APOCALYPTICISMS",
		"APOLIPOPROTEINS", "APOLITICALITIES", "APOPHTHEGMATISE", "APOPHTHEGMATIST", "APOPHTHEGMATIZE", "APOTHEGMATISING", "APOTHEGMATIZING", "APPEALABILITIES",
		"APPEALINGNESSES", "APPENDICULARIAN", "APPLICABILITIES", "APPRENTICEHOODS", "APPRENTICEMENTS", "APPRENTICESHIPS", "APPROACHABILITY", "APPROPINQUATING",
		"APPROPINQUATION", "APPROPINQUITIES", "APPROPRIATENESS", "ARACHNOIDITISES", "ARBITRARINESSES", "ARBORICULTURIST", "ARCHAEBACTERIUM", "ARCHAEOBOTANIES",
		"ARCHAEOBOTANIST", "ARCHAEOMETRISTS", "ARCHAEOPTERYXES", "ARCHAEZOOLOGIES", "ARCHEOASTRONOMY", "ARCHEOBOTANISTS", "ARCHEOLOGICALLY", "ARCHEOMAGNETISM",
		"ARCHEOZOOLOGIES", "ARCHEOZOOLOGIST", "ARCHGENETHLIACS", "ARCHIDIACONATES", "ARCHIEPISCOPACY", "ARCHIEPISCOPATE", "ARCHITECTURALLY", "ARCHPRIESTHOODS",
		"ARCHPRIESTSHIPS", "ARGUMENTATIVELY", "ARIBOFLAVINOSES", "ARIBOFLAVINOSIS", "AROMATHERAPISTS", "ARRONDISSEMENTS", "ARTERIALISATION", "ARTERIALIZATION",
		"ARTERIOGRAPHIES", "ARTIFICIALISING", "ARTIFICIALITIES", "ARTIFICIALIZING", "ASCLEPIADACEOUS", "ASSENTIVENESSES"}

	for i := 0; i < b.N; i++ {
		_ = CopyIn("temp", bigTableColumns...)
	}
}
