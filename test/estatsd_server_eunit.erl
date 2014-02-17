-module(estatsd_server_eunit).

-include_lib("eunit/include/eunit.hrl").

key2str_test_() ->
    [
        ?_assertMatch("binary.is.allowed",
            estatsd_server:key2str(<<"binary.is.allowed">>)),
        ?_assertMatch("atom.is.allowed",
            estatsd_server:key2str('atom.is.allowed')),
        ?_assertMatch("abc.def.ghi.jkl.mno.pqr.stu.vwx.yz",
            estatsd_server:key2str("abc.def.ghi.jkl.mno.pqr.stu.vwx.yz")),
        ?_assertMatch("ABC-DEF-GHI.JKL_MNO_PQR_STU_VWX_YZ",
            estatsd_server:key2str("ABC/DEF/GHI.JKL MNO PQR STU VWX YZ")),
        ?_assertMatch("key.with.0123456789.is.also.allowed",
            estatsd_server:key2str("key.with.0123456789.is.also.allowed")),
        ?_assertMatch("and.some.are.not",
            estatsd_server:key2str("and.some.!@#$%^&*()are.not"))
    ].
