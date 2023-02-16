var gulp = require('gulp');
const zip = require('gulp-zip');
var jshint = require('gulp-jshint');

var lintSrc = [
  './controllers/*.js',
  './dao/*.js',
  './model/*.js',
  './util/*.js',
  './config.js',
  './datamart.js'
];

gulp.task('lint', function () {
  return gulp.src(lintSrc)
    .pipe(jshint())
    .pipe(jshint.reporter('default', {verbose: true}));
});
gulp.task('default', () =>
    gulp.src('DatamartAlarmAndEvents/**/*')
        .pipe(zip('archive.zip'))
        .pipe(gulp.dest('/'))
);