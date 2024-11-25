package shell

import (
	"fmt"
	"io"
	"strings"
	"time"
)

func init() {
	Commands = append(Commands, &commandFsLookupFileId{})
}

type commandFsLookupFileId struct {
}

func (c *commandFsLookupFileId) Name() string {
	return "fs.lookupFileId"
}

func (c *commandFsLookupFileId) Help() string {
	return `Lookup File Id Info on to the screen

	fs.lookupFileId fileId
`
}

var getLookupFileIdBackoffSchedule = []time.Duration{
	150 * time.Millisecond,
	600 * time.Millisecond,
	1800 * time.Millisecond,
}

func (c *commandFsLookupFileId) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsLookupFileId) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	fileId := args[len(args)-1]

	if len(fileId) <= 0 || !strings.Contains(fileId, ",") {
		return fmt.Errorf("%s format error", fileId)
	}

	var urlStrings []string
	var err0 error
	for _, backoff := range getLookupFileIdBackoffSchedule {
		urlStrings, err0 = commandEnv.MasterClient.GetLookupFileIdFunction()(fileId)
		if err0 == nil && len(urlStrings) > 0 {
			break
		}
		time.Sleep(backoff)
	}
	if err0 != nil {
		return err0
	} else if len(urlStrings) == 0 {
		errUrlNotFound := fmt.Errorf("operation LookupFileId %s failed, err: urls not found", fileId)
		return errUrlNotFound
	}

	_, err0 = fmt.Fprintf(writer, "urls:%v \n", urlStrings)
	if err0 != nil {
		return err0
	}
	return nil
}
